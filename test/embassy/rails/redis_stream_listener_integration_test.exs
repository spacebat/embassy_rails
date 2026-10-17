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

  test "recreates a destroyed group", ctx do
    start_listener(ctx, consumer: "c1")
    assert_receive {:redisx, :connected, _, _}, 1_000
    Phoenix.PubSub.subscribe(__MODULE__.PubSub, Broadcast.topic("back"))

    Redix.command!(ctx.redis, ["XGROUP", "DESTROY", ctx.stream, "bx"])

    # A recreated group starts at `$` (as in anycable-go), so wait for it
    # before adding the entry.
    eventually(fn ->
      ctx.redis
      |> Redix.command!(["XINFO", "GROUPS", ctx.stream])
      |> Enum.any?(&match?(["name", "bx" | _], &1))
    end)

    xadd_payload(ctx.redis, ctx.stream, envelope("back", "again"))
    assert_receive {:cable_broadcast, "back", %{data: "again"}}, 1_000
    # NOGROUP is a regroup, not a connection failure.
    refute_received {:redisx, :disconnected, _, _}
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

  test "removes its consumer on shutdown", ctx do
    start_listener(ctx, consumer: "c1")
    assert_receive {:redisx, :connected, _, _}, 1_000

    stop_supervised!("c1")

    consumers = Redix.command!(ctx.redis, ["XINFO", "CONSUMERS", ctx.stream, "bx"])
    refute Enum.any?(consumers, &match?(["name", "c1" | _], &1))
  end
end
