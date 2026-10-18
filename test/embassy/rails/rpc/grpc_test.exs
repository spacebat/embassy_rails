defmodule Embassy.Rails.RPC.GrpcTest do
  use ExUnit.Case, async: true

  alias Embassy.Rails.RPC.Grpc

  describe "normalize_connect/1" do
    test "maps a SUCCESS ConnectionResponse to the HTTP-shaped map with integer status" do
      resp = %Anycable.ConnectionResponse{
        status: :SUCCESS,
        identifiers: ~s({"uid":1}),
        transmissions: [~s({"type":"welcome"})],
        env: %Anycable.EnvResponse{cstate: %{"k" => "v"}, istate: %{"i" => "1"}}
      }

      assert Grpc.normalize_connect(resp) == %{
               "status" => 1,
               "identifiers" => ~s({"uid":1}),
               "transmissions" => [~s({"type":"welcome"})],
               "env" => %{"cstate" => %{"k" => "v"}, "istate" => %{"i" => "1"}}
             }
    end

    test "maps a FAILURE status to integer 2 and tolerates a nil env" do
      resp = %Anycable.ConnectionResponse{
        status: :FAILURE,
        identifiers: "",
        transmissions: [],
        env: nil
      }

      normalized = Grpc.normalize_connect(resp)
      assert normalized["status"] == 2
      assert normalized["env"] == %{"cstate" => %{}, "istate" => %{}}
    end

    test "passes through an integer status value unchanged" do
      resp = %Anycable.ConnectionResponse{status: 0, identifiers: "", transmissions: [], env: nil}
      assert Grpc.normalize_connect(resp)["status"] == 0
    end
  end

  describe "normalize_command/1" do
    test "maps a CommandResponse including streams and booleans" do
      resp = %Anycable.CommandResponse{
        status: :SUCCESS,
        disconnect: false,
        stop_streams: true,
        streams: ["chat:1"],
        transmissions: [~s({"type":"confirm"})],
        stopped_streams: ["chat:0"],
        env: %Anycable.EnvResponse{cstate: %{}, istate: %{}}
      }

      assert Grpc.normalize_command(resp) == %{
               "status" => 1,
               "disconnect" => false,
               "stop_streams" => true,
               "streams" => ["chat:1"],
               "transmissions" => [~s({"type":"confirm"})],
               "stopped_streams" => ["chat:0"],
               "env" => %{"cstate" => %{}, "istate" => %{}},
               "presence" => nil
             }
    end

    test "maps presence" do
      resp = %Anycable.CommandResponse{
        status: :SUCCESS,
        presence: %Anycable.PresenceResponse{type: "join", id: "2024", info: ~s({"name":"Bart"})}
      }

      assert %{"presence" => %{"type" => "join", "id" => "2024", "info" => ~s({"name":"Bart"})}} =
               Grpc.normalize_command(resp)
    end

    test "maps empty info to nil and absent presence to nil" do
      leave = %Anycable.CommandResponse{
        status: :SUCCESS,
        presence: %Anycable.PresenceResponse{type: "leave", id: "1"}
      }

      assert Grpc.normalize_command(leave)["presence"] == %{
               "type" => "leave",
               "id" => "1",
               "info" => nil
             }

      assert Grpc.normalize_command(%Anycable.CommandResponse{status: :SUCCESS})["presence"] ==
               nil
    end
  end

  describe "build_connect_request/1" do
    test "builds a ConnectionRequest with stringified env maps" do
      env = %{url: "wss://t/", headers: %{}, cstate: %{"a" => "b"}, istate: %{}}

      req = Grpc.build_connect_request(env)
      assert %Anycable.ConnectionRequest{env: %Anycable.Env{} = e} = req
      assert e.url == "wss://t/"
      assert e.cstate == %{"a" => "b"}
    end
  end

  describe "normalize_disconnect/1" do
    test "maps a SUCCESS DisconnectResponse to a status map" do
      resp = %Anycable.DisconnectResponse{status: :SUCCESS}
      assert Grpc.normalize_disconnect(resp) == %{"status" => 1}
    end
  end

  describe "build_disconnect_request/1" do
    test "builds a DisconnectRequest from a payload map" do
      payload = %{
        identifiers: ~s({"uid":1}),
        subscriptions: [~s({"channel":"ChatChannel"})],
        env: %{url: "wss://t/", headers: %{}, cstate: %{}, istate: %{}}
      }

      req = Grpc.build_disconnect_request(payload)
      assert %Anycable.DisconnectRequest{identifiers: ~s({"uid":1})} = req
      assert req.subscriptions == [~s({"channel":"ChatChannel"})]
    end
  end

  describe "build_command_request/1" do
    test "maps a command payload to a CommandMessage" do
      payload = %{
        command: "subscribe",
        identifier: ~s({"channel":"X"}),
        connection_identifiers: ~s({"uid":1}),
        data: "",
        env: %{url: "wss://t/", headers: %{}, cstate: %{}, istate: %{}}
      }

      msg = Grpc.build_command_request(payload)
      assert %Anycable.CommandMessage{command: "subscribe", identifier: ~s({"channel":"X"})} = msg
      assert msg.connection_identifiers == ~s({"uid":1})
    end
  end

  describe "call/3 without a published channel" do
    setup do
      instance = :"grpc_rpc_#{System.unique_integer([:positive])}"
      start_supervised!({Registry, keys: :unique, name: Embassy.Rails.registry_name(instance)})
      {:ok, instance: instance}
    end

    test "returns {:error, :no_channel} when the holder has not connected", %{instance: instance} do
      req = Grpc.build_connect_request(%{url: "wss://t/", headers: %{}, cstate: %{}, istate: %{}})
      assert {:error, :no_channel} = Grpc.call(instance, :connect, req)
    end
  end

  describe "connection_lost?/1" do
    test "a closed connection or an unavailable server needs a new channel" do
      assert Grpc.connection_lost?(
               {:grpc, 2,
                ~s(error occurred while receiving data: {:error, "the connection is closed"})}
             )

      assert Grpc.connection_lost?({:grpc, 14, "unavailable"})
      assert Grpc.connection_lost?({:grpc, :closed})
    end

    test "a deadline or another status keeps the channel" do
      refute Grpc.connection_lost?({:grpc, 4, "deadline exceeded"})
      refute Grpc.connection_lost?({:grpc, 2, "boom"})
      refute Grpc.connection_lost?({:grpc, 16, "unauthenticated"})
    end
  end
end
