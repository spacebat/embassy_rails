defmodule Embassy.Rails.GrpcChannelTest do
  use ExUnit.Case, async: true

  alias Embassy.Rails.GrpcChannel
  alias Embassy.Rails.Test.GrpcServer
  alias Embassy.Rails.Test.Helpers

  setup do
    instance = :"grpc_chan_#{System.unique_integer([:positive])}"
    # The holder publishes into the instance's Registry; start one.
    start_supervised!({Registry, keys: :unique, name: Embassy.Rails.registry_name(instance)})
    {:ok, instance: instance}
  end

  test "publishes the channel to Registry meta on successful connect", %{instance: instance} do
    # Start the in-test gRPC server on an ephemeral TCP port (127.0.0.1:0).
    # UDS via the Mint adapter is not supported in grpc 1.0; see test/support/grpc_server.ex.
    {:ok, endpoint} = GrpcServer.start("ignored")

    start_supervised!({GrpcChannel, instance: instance, endpoint: endpoint, secret: "shh"})

    assert Helpers.eventually(fn ->
             match?({:ok, %GRPC.Channel{}}, GrpcChannel.fetch_channel(instance))
           end)
  end

  test "stays alive and leaves the channel unset when connect fails", %{instance: instance} do
    # Nothing listening on this TCP address.
    bad = "127.0.0.1:1"

    pid =
      start_supervised!({GrpcChannel, instance: instance, endpoint: bad, secret: "shh"})

    Process.sleep(100)
    assert Process.alive?(pid)
    assert GrpcChannel.fetch_channel(instance) == :error
  end

end
