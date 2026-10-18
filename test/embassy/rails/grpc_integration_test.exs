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

    assert Helpers.eventually(fn -> match?({:ok, _}, RPC.connect(instance, env)) end)
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
    :sys.get_state(GrpcChannel.whereis(instance))

    assert [{_, _, %{instance: ^instance}}] = TelemetryCollector.events(collector)
    assert {:ok, after_burst} = GrpcChannel.fetch_channel(instance)
    assert after_burst != before
  end
end
