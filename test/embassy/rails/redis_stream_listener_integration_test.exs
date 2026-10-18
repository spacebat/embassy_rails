defmodule Embassy.Rails.RedisStreamListenerIntegrationTest do
  use Embassy.Rails.Test.RedisCase, async: false

  alias Embassy.Rails.{Broadcast, RedisStreamListener}

  @events [:connected, :disconnected, :reconnected, :claimed, :exception]

  setup %{redis_url: url} do
    start_supervised!({Finch, name: __MODULE__.Finch})
    start_supervised!({Phoenix.PubSub, name: __MODULE__.PubSub})
    start_supervised!({Embassy, name: __MODULE__.Embassy, finch: __MODULE__.Finch})

    start_supervised!(
      {Embassy.Rails,
       name: __MODULE__.Instance,
       embassy: __MODULE__.Embassy,
       pubsub: __MODULE__.PubSub,
       rpc: [transport: {:http, "http://example.test"}, secret: "shh"],
       broadcast: [source: :http, key: "bcast"]}
    )

    test_pid = self()
    handler = "redisx-test-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler,
      Enum.map(@events, &[:embassy_rails, :broadcast, :redisx, &1]),
      &__MODULE__.forward_event/4,
      test_pid
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    {:ok, instance: __MODULE__.Instance, url: url}
  end

  def forward_event([_, _, :redisx, name], measurements, metadata, pid) do
    send(pid, {:redisx, name, measurements, metadata})
  end

  defp start_listener(ctx, opts) do
    defaults = [instance: ctx.instance, redis: ctx.url, stream: ctx.stream, block_ms: 100]
    opts = Keyword.merge(defaults, opts)
    id = Keyword.get(opts, :consumer, :listener)
    start_supervised!(Supervisor.child_spec({RedisStreamListener, opts}, id: id))
  end

  defp eventually(fun, tries \\ 50) do
    cond do
      fun.() -> :ok
      tries == 0 -> flunk("condition never became true")
      true -> Process.sleep(20) && eventually(fun, tries - 1)
    end
  end

  defp envelope(stream, data), do: Jason.encode!(%{stream: stream, data: data})

  test "dispatches an XADDed envelope, then acks and deletes it", ctx do
    start_listener(ctx, consumer: "c1")
    assert_receive {:redisx, :connected, _, %{consumer: "c1"}}, 1_000
    Phoenix.PubSub.subscribe(__MODULE__.PubSub, Broadcast.topic("chat:1"))

    xadd_payload(ctx.redis, ctx.stream, envelope("chat:1", "hi"))

    assert_receive {:cable_broadcast, "chat:1", %{data: "hi"}}, 1_000
    eventually(fn -> Redix.command!(ctx.redis, ["XLEN", ctx.stream]) == 0 end)
    assert [0 | _] = Redix.command!(ctx.redis, ["XPENDING", ctx.stream, "bx"])
  end

  test "uses group bx by default", ctx do
    start_listener(ctx, consumer: "c1")
    assert_receive {:redisx, :connected, _, %{group: "bx"}}, 1_000

    groups = Redix.command!(ctx.redis, ["XINFO", "GROUPS", ctx.stream])
    assert Enum.any?(groups, &match?(["name", "bx" | _], &1))
  end

  test "two consumers in one group dispatch each entry once", ctx do
    start_listener(ctx, consumer: "c1")
    start_listener(ctx, consumer: "c2")
    assert_receive {:redisx, :connected, _, _}, 1_000
    assert_receive {:redisx, :connected, _, _}, 1_000
    Phoenix.PubSub.subscribe(__MODULE__.PubSub, Broadcast.topic("n"))

    for i <- 0..19, do: xadd_payload(ctx.redis, ctx.stream, envelope("n", to_string(i)))

    received = collect_broadcasts("n", [])
    assert Enum.sort(received) == Enum.sort(Enum.map(0..19, &to_string/1))
  end

  defp collect_broadcasts(stream, acc) do
    receive do
      {:cable_broadcast, ^stream, %{data: data}} -> collect_broadcasts(stream, [data | acc])
    after
      1_000 -> acc
    end
  end

  test "reclaims entries left pending by a dead consumer", ctx do
    Redix.command!(ctx.redis, ["XGROUP", "CREATE", ctx.stream, "bx", "$", "MKSTREAM"])
    xadd_payload(ctx.redis, ctx.stream, envelope("ghosted", "boo"))
    Redix.command!(ctx.redis, ["XREADGROUP", "GROUP", "bx", "ghost", "STREAMS", ctx.stream, ">"])
    Phoenix.PubSub.subscribe(__MODULE__.PubSub, Broadcast.topic("ghosted"))

    start_listener(ctx, consumer: "c1")

    assert_receive {:cable_broadcast, "ghosted", %{data: "boo"}}, 1_000
    assert_receive {:redisx, :claimed, %{count: 1}, %{consumer: "c1"}}, 1_000
  end

  test "recreates a destroyed group without losing entries added before it is back", ctx do
    start_listener(ctx, consumer: "c1")
    assert_receive {:redisx, :connected, _, _}, 1_000
    Phoenix.PubSub.subscribe(__MODULE__.PubSub, Broadcast.topic("back"))

    # The entry lands while no group exists, before the listener regroups.
    Redix.transaction_pipeline!(ctx.redis, [
      ["XGROUP", "DESTROY", ctx.stream, "bx"],
      ["XADD", ctx.stream, "*", "payload", envelope("back", "again")]
    ])

    assert_receive {:cable_broadcast, "back", %{data: "again"}}, 1_000
    # NOGROUP is a regroup, not a connection failure.
    refute_received {:redisx, :disconnected, _, _}
  end

  test "regroups when the stream key is deleted under a blocked read", ctx do
    start_listener(ctx, consumer: "c1", block_ms: 1_000)
    assert_receive {:redisx, :connected, _, _}, 1_000
    Phoenix.PubSub.subscribe(__MODULE__.PubSub, Broadcast.topic("gone"))
    # Let the listener block in XREADGROUP.
    Process.sleep(100)

    Redix.transaction_pipeline!(ctx.redis, [
      ["DEL", ctx.stream],
      ["XADD", ctx.stream, "*", "payload", envelope("gone", "kept")]
    ])

    assert_receive {:cable_broadcast, "gone", %{data: "kept"}}, 2_000
    refute_received {:redisx, :disconnected, _, _}
  end

  test "dispatches a batch that contains a command entry", ctx do
    listener = start_listener(ctx, consumer: "c1")
    assert_receive {:redisx, :connected, _, _}, 1_000
    Phoenix.PubSub.subscribe(__MODULE__.PubSub, Broadcast.topic("m"))

    batch =
      Jason.encode!([
        %{stream: "m", data: "1"},
        %{command: "disconnect", payload: %{identifier: "u1", reconnect: true}},
        %{stream: "m", data: "2"}
      ])

    xadd_payload(ctx.redis, ctx.stream, batch)

    assert_receive {:cable_broadcast, "m", %{data: "1"}}, 1_000
    assert_receive {:cable_broadcast, "m", %{data: "2"}}, 1_000
    eventually(fn -> Redix.command!(ctx.redis, ["XLEN", ctx.stream]) == 0 end)
    assert Process.alive?(listener)
  end

  test "acks and drops an entry without a payload field", ctx do
    start_listener(ctx, consumer: "c1")
    assert_receive {:redisx, :connected, _, _}, 1_000

    Redix.command!(ctx.redis, ["XADD", ctx.stream, "*", "other", "x"])

    assert_receive {:redisx, :exception, _, %{kind: :missing_payload, id: id}}, 1_000
    assert is_binary(id)
    eventually(fn -> Redix.command!(ctx.redis, ["XLEN", ctx.stream]) == 0 end)
  end

  test "acks and drops an undecodable payload", ctx do
    start_listener(ctx, consumer: "c1")
    assert_receive {:redisx, :connected, _, _}, 1_000

    xadd_payload(ctx.redis, ctx.stream, "not json")

    assert_receive {:redisx, :exception, _, %{kind: :bad_envelope}}, 1_000
    eventually(fn -> Redix.command!(ctx.redis, ["XLEN", ctx.stream]) == 0 end)
  end

  test "recovers after the Redis connection drops", ctx do
    listener = start_listener(ctx, consumer: "c1")
    assert_receive {:redisx, :connected, _, _}, 1_000
    Phoenix.PubSub.subscribe(__MODULE__.PubSub, Broadcast.topic("rc"))

    %{conn: conn} = :sys.get_state(listener)
    Process.exit(conn, :kill)

    assert_receive {:redisx, :disconnected, _, %{consumer: "c1"}}, 1_000
    xadd_payload(ctx.redis, ctx.stream, envelope("rc", "after"))

    assert_receive {:redisx, :reconnected, _, %{consumer: "c1"}}, 3_000
    assert_receive {:cable_broadcast, "rc", %{data: "after"}}, 3_000
  end

  test "an Embassy.Rails instance with source :redisx delivers broadcasts", ctx do
    start_supervised!(
      {Embassy.Rails,
       name: __MODULE__.RedisxInstance,
       embassy: __MODULE__.Embassy,
       pubsub: __MODULE__.PubSub,
       rpc: [transport: {:http, "http://example.test"}, secret: "shh"],
       broadcast: [source: :redisx, redis: ctx.url, stream: ctx.stream, block_ms: 100]}
    )

    assert_receive {:redisx, :connected, _, %{instance: __MODULE__.RedisxInstance}}, 1_000
    Phoenix.PubSub.subscribe(__MODULE__.PubSub, Broadcast.topic("wired"))

    xadd_payload(ctx.redis, ctx.stream, envelope("wired", "up"))
    assert_receive {:cable_broadcast, "wired", %{data: "up"}}, 1_000
  end

  test "removes its consumer on shutdown", ctx do
    start_listener(ctx, consumer: "c1")
    assert_receive {:redisx, :connected, _, _}, 1_000

    stop_supervised!("c1")

    consumers = Redix.command!(ctx.redis, ["XINFO", "CONSUMERS", ctx.stream, "bx"])
    refute Enum.any?(consumers, &match?(["name", "c1" | _], &1))
  end

  test "an instance is ready only once its listener is consuming", ctx do
    bypass = Bypass.open()
    Bypass.stub(bypass, "HEAD", "/", fn conn -> Plug.Conn.resp(conn, 404, "") end)

    start_supervised!(
      {Embassy.Rails,
       name: __MODULE__.ReadyInstance,
       embassy: __MODULE__.Embassy,
       pubsub: __MODULE__.PubSub,
       rpc: [transport: {:http, "http://localhost:#{bypass.port}"}, secret: "shh"],
       broadcast: [source: :redisx, redis: ctx.url, stream: ctx.stream, block_ms: 100]}
    )

    assert_receive {:redisx, :connected, _, %{instance: __MODULE__.ReadyInstance}}, 1_000
    assert Embassy.Rails.Health.ready?(__MODULE__.ReadyInstance)
  end

  defp drain(instance) do
    start_supervised!({Embassy.Rails.Drain, instance: instance, window_ms: 10, batches: 1})
    stop_supervised!({Embassy.Rails.Drain, instance})
  end

  defp consumer?(ctx, name) do
    ctx.redis
    |> Redix.command!(["XINFO", "CONSUMERS", ctx.stream, "bx"])
    |> Enum.any?(&match?(["name", ^name | _], &1))
  end

  test "stops reading and removes its consumer once draining begins", ctx do
    pid = start_listener(ctx, consumer: "c1")
    assert_receive {:redisx, :connected, _, _}, 1_000
    Phoenix.PubSub.subscribe(__MODULE__.PubSub, Broadcast.topic("drained"))

    drain(ctx.instance)
    eventually(fn -> not consumer?(ctx, "c1") end)

    xadd_payload(ctx.redis, ctx.stream, envelope("drained", "late"))
    refute_receive {:cable_broadcast, "drained", _}, 500
    assert Redix.command!(ctx.redis, ["XLEN", ctx.stream]) == 1
    assert Process.alive?(pid)
    refute Embassy.Rails.Health.source_ready?(ctx.instance)
  end

  # Single-node stand-in for two nodes: each instance has its own PubSub,
  # as if the draining node were already cut off from its peers.
  test "broadcasting through a drain loses nothing, and after it only the other node reads",
       ctx do
    start_supervised!({Phoenix.PubSub, name: __MODULE__.OtherPubSub}, id: :other_pubsub)

    start_supervised!(
      {Embassy.Rails,
       name: __MODULE__.Other,
       embassy: __MODULE__.Embassy,
       pubsub: __MODULE__.OtherPubSub,
       rpc: [transport: {:http, "http://example.test"}, secret: "shh"],
       broadcast: [source: :http, key: "bcast"]},
      id: :other_instance
    )

    start_listener(ctx, consumer: "draining")
    start_listener(%{ctx | instance: __MODULE__.Other}, consumer: "staying")
    assert_receive {:redisx, :connected, _, %{consumer: "draining"}}, 1_000
    assert_receive {:redisx, :connected, _, %{consumer: "staying"}}, 1_000

    # Every entry is a batch for topics "d" and "s", but the test hears "d"
    # only on the draining node's PubSub and "s" only on the other's, so
    # each receipt names the node that read the entry.
    Phoenix.PubSub.subscribe(__MODULE__.PubSub, Broadcast.topic("d"))
    Phoenix.PubSub.subscribe(__MODULE__.OtherPubSub, Broadcast.topic("s"))
    test_pid = self()

    producer =
      Task.async(fn ->
        for i <- 1..40 do
          batch = [%{stream: "d", data: "#{i}"}, %{stream: "s", data: "#{i}"}]
          xadd_payload(ctx.redis, ctx.stream, Jason.encode!(batch))
          send(test_pid, {:added, i, System.monotonic_time(:millisecond)})
          if i == 10, do: send(test_pid, :drain_now)
          Process.sleep(10)
        end
      end)

    assert_receive :drain_now, 1_000
    drain(ctx.instance)
    eventually(fn -> not consumer?(ctx, "draining") end)
    removed_at = System.monotonic_time(:millisecond)
    Task.await(producer)

    for i <- 1..40 do
      data = "#{i}"
      assert_receive {:added, ^i, added_at}

      receive do
        {:cable_broadcast, topic, %{data: ^data}} ->
          if added_at > removed_at, do: assert(topic == "s", "entry #{i} read while draining")
      after
        2_000 -> flunk("entry #{i} was lost")
      end
    end

    refute_receive {:cable_broadcast, _, _}, 100
  end
end
