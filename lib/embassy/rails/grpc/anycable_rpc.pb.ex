# grpc and protobuf are optional deps; see priv/protos/README.md.
if Code.ensure_loaded?(GRPC.Stub) do
  defmodule Anycable.RPC.Service do
    @moduledoc false
    use GRPC.Service, name: "anycable.RPC", protoc_gen_elixir_version: "0.17.0"

    rpc(:Connect, Anycable.ConnectionRequest, Anycable.ConnectionResponse)

    rpc(:Command, Anycable.CommandMessage, Anycable.CommandResponse)

    rpc(:Disconnect, Anycable.DisconnectRequest, Anycable.DisconnectResponse)
  end

  defmodule Anycable.RPC.Stub do
    @moduledoc false
    use GRPC.Stub, service: Anycable.RPC.Service
  end
end
