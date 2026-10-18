defmodule Embassy.Rails.GrpcIntegrationTest do
  # async: false because GrpcServer starts a Cowboy listener under a fixed
  # module-derived name; concurrent test runs would collide on that name.
  use ExUnit.Case, async: false

  alias Embassy.Rails.GrpcChannel
  alias Embassy.Rails.Health
  alias Embassy.Rails.RPC
  alias Embassy.Rails.Test.GrpcServer
  alias Embassy.Rails.Test.Helpers
  alias Embassy.Rails.Test.TelemetryCollector

  setup do
    instance = :"grpc_int_#{System.unique_integer([:positive])}"
    {:ok, endpoint} = GrpcServer.start("ignored")

    start_supervised!({Finch, name: :"#{instance}.Finch"})
    start_supervised!({Phoenix.PubSub, name: :"#{instance}.PubSub"})
    start_supervised!({Embassy, name: :"#{instance}.Embassy", finch: :"#{instance}.Finch"})

    start_supervised!(
      {Embassy.Rails,
       name: instance,
       embassy: :"#{instance}.Embassy",
       pubsub: :"#{instance}.PubSub",
       rpc: [transport: {:grpc, endpoint}, secret: "shh"],
       broadcast: [source: :http, key: "bcast"]}
    )

    assert Helpers.eventually(fn ->
             match?({:ok, _}, GrpcChannel.fetch_channel(instance))
           end)

    {:ok, instance: instance, endpoint: endpoint}
  end

  test "connect returns the normalized success map", %{instance: instance} do
    assert {:ok, resp} =
             RPC.connect(instance, %{url: "wss://t/", headers: %{}, cstate: %{}, istate: %{}})

    assert resp["status"] == 1
    assert resp["identifiers"] == ~s({"uid":1})
    assert resp["transmissions"] == [~s({"type":"welcome"})]
    assert get_in(resp, ["env", "cstate"]) == %{"k" => "v"}
  end

  test "command returns streams and transmissions", %{instance: instance} do
    payload = %{
      command: "subscribe",
      identifier: ~s({"channel":"ChatChannel"}),
      connection_identifiers: ~s({"uid":1}),
      data: "",
      env: %{url: "wss://t/", headers: %{}, cstate: %{}, istate: %{}}
    }

    assert {:ok, resp} = RPC.command(instance, payload)
    assert resp["status"] == 1
    assert resp["streams"] == ["chat:1"]
  end

  @tag :capture_log
  test "readiness is false once the gRPC server is gone, with a channel still cached",
       %{instance: instance} do
    assert Health.ready?(instance)

    :ok = GRPC.Server.stop(GrpcServer.Impl)

    refute Health.ready?(instance)
  end

  @tag :capture_log
  test "calls recover once a restarted gRPC server is back on the same port",
       %{instance: instance, endpoint: endpoint} do
    env = %{url: "wss://t/", headers: %{}, cstate: %{}, istate: %{}}
    assert {:ok, _} = RPC.connect(instance, env)

    :ok = GRPC.Server.stop(GrpcServer.Impl)
    assert {:error, _} = RPC.connect(instance, env)

    [_host, port] = String.split(endpoint, ":")
    {:ok, _pid, _port} = GRPC.Server.start(GrpcServer.Impl, String.to_integer(port))

    # Covers several GrpcChannel backoff steps (200, 400, 800 ms).
    assert Helpers.eventually(fn -> match?({:ok, _}, RPC.connect(instance, env)) end, 250)
  end

  @tag :capture_log
  test "a dead connection process fails the call and is replaced", %{instance: instance} do
    {:ok, %{adapter_payload: %{conn_pid: conn_pid}} = before} =
      GrpcChannel.fetch_channel(instance)

    # Suspended, the holder cannot replace the channel before the call
    # hits the dead process.
    holder = GrpcChannel.whereis(instance)
    :sys.suspend(holder)
    Process.exit(conn_pid, :kill)

    env = %{url: "wss://t/", headers: %{}, cstate: %{}, istate: %{}}
    assert {:error, {:grpc, {:exit, _}}} = RPC.connect(instance, env)
    :sys.resume(holder)

    assert Helpers.eventually(
             fn -> match?({:ok, ch} when ch != before, GrpcChannel.fetch_channel(instance)) end,
             100
           )

    assert {:ok, _} = RPC.connect(instance, env)
  end

  @tag :capture_log
  test "a dead connection process is replaced without waiting for a call",
       %{instance: instance} do
    {:ok, %{adapter_payload: %{conn_pid: conn_pid}} = before} =
      GrpcChannel.fetch_channel(instance)

    Process.exit(conn_pid, :kill)

    assert Helpers.eventually(
             fn -> match?({:ok, ch} when ch != before, GrpcChannel.fetch_channel(instance)) end,
             100
           )
  end

  @tag :capture_log
  test "a config Registry restart closes the old connection", %{instance: instance} do
    {:ok, before} = GrpcChannel.fetch_channel(instance)
    children_before = DynamicSupervisor.count_children(GRPC.Client.Supervisor).active

    instance |> Embassy.Rails.registry_name() |> Process.whereis() |> Process.exit(:kill)

    assert Helpers.eventually(
             fn -> match?({:ok, ch} when ch != before, GrpcChannel.fetch_channel(instance)) end,
             100
           )

    assert DynamicSupervisor.count_children(GRPC.Client.Supervisor).active == children_before
  end

  @tag :capture_log
  test "a reconnect closes the previous connection", %{instance: instance} do
    sup = GRPC.Client.Supervisor
    children_before = DynamicSupervisor.count_children(sup).active

    for _ <- 1..5 do
      GrpcChannel.reconnect(instance)
      :sys.get_state(GrpcChannel.whereis(instance))
    end

    assert {:ok, %GRPC.Channel{}} = GrpcChannel.fetch_channel(instance)
    assert DynamicSupervisor.count_children(sup).active == children_before
  end

  @tag :capture_log
  test "a burst of reconnect requests opens one connection", %{instance: instance} do
    {:ok, collector} = TelemetryCollector.attach([[:embassy_rails, :rpc, :grpc, :connected]])
    {:ok, before} = GrpcChannel.fetch_channel(instance)

    for _ <- 1..20, do: GrpcChannel.reconnect(instance, before)

    assert Helpers.eventually(
             fn -> match?({:ok, ch} when ch != before, GrpcChannel.fetch_channel(instance)) end,
             100
           )

    assert [{_, _, %{instance: ^instance}}] = TelemetryCollector.events(collector)
  end

  # A proxy in front of a dead RPC server answers every call UNAVAILABLE
  # on a channel that connected fine: each fresh channel fails at once.
  @tag :capture_log
  test "reconnects for a channel that keeps failing are at most one a second",
       %{instance: instance} do
    {:ok, collector} = TelemetryCollector.attach([[:embassy_rails, :rpc, :grpc, :connected]])
    deadline = System.monotonic_time(:millisecond) + 1_500

    Stream.repeatedly(fn -> System.monotonic_time(:millisecond) end)
    |> Stream.take_while(&(&1 < deadline))
    |> Enum.each(fn _ ->
      with {:ok, ch} <- GrpcChannel.fetch_channel(instance),
           do: GrpcChannel.reconnect(instance, ch)

      Process.sleep(10)
    end)

    assert collector |> TelemetryCollector.events() |> Enum.count_until(3) <= 2
  end
end
