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
               "env" => %{"cstate" => %{}, "istate" => %{}}
             }
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
end
