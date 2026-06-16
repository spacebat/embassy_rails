defmodule Embassy.Rails.GrpcIntegrationTest do
  # async: false because GrpcServer starts a Cowboy listener under a fixed
  # module-derived name; concurrent test runs would collide on that name.
  use ExUnit.Case, async: false

  alias Embassy.Rails.GrpcChannel
  alias Embassy.Rails.RPC
  alias Embassy.Rails.Test.GrpcServer

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

    assert eventually(fn ->
             match?({:ok, _}, GrpcChannel.fetch_channel(instance))
           end)

    {:ok, instance: instance}
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

  defp eventually(fun, tries \\ 50)
  defp eventually(_fun, 0), do: false
  defp eventually(fun, tries), do: fun.() || (Process.sleep(20) && eventually(fun, tries - 1))
end
