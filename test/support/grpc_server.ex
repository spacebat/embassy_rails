defmodule Embassy.Rails.Test.GrpcServer do
  @moduledoc false

  # GRPC.Server.start/3 returns {:ok, pid, port} (3-tuple).
  # The Cowboy adapter supports UDS via adapter_opts: [ip: {:local, path}],
  # but the Mint client adapter does not (see start/1 doc below), so we use TCP.

  defmodule Impl do
    @moduledoc false
    use GRPC.Server, service: Anycable.RPC.Service

    @spec connect(Anycable.ConnectionRequest.t(), GRPC.Server.Stream.t()) ::
            Anycable.ConnectionResponse.t()
    def connect(_request, stream) do
      # anycable-rpc answers ERROR to a client that doesn't send protov.
      if GRPC.Stream.get_headers(stream)["protov"] == "v1",
        do: success_connect(),
        else: %Anycable.ConnectionResponse{status: :ERROR, error_msg: "incompatible client"}
    end

    defp success_connect do
      %Anycable.ConnectionResponse{
        status: :SUCCESS,
        identifiers: ~s({"uid":1}),
        transmissions: [~s({"type":"welcome"})],
        env: %Anycable.EnvResponse{cstate: %{"k" => "v"}, istate: %{}}
      }
    end

    @spec command(Anycable.CommandMessage.t(), GRPC.Server.Stream.t()) ::
            Anycable.CommandResponse.t()
    def command(_request, _stream) do
      %Anycable.CommandResponse{
        status: :SUCCESS,
        streams: ["chat:1"],
        transmissions: [~s({"type":"confirm_subscription"})],
        env: %Anycable.EnvResponse{cstate: %{}, istate: %{}}
      }
    end

    @spec disconnect(Anycable.DisconnectRequest.t(), GRPC.Server.Stream.t()) ::
            Anycable.DisconnectResponse.t()
    def disconnect(_request, _stream) do
      %Anycable.DisconnectResponse{status: :SUCCESS}
    end
  end

  @doc """
  Start the gRPC server on an ephemeral TCP port.

  `sock_path` is accepted for API compatibility but ignored; the server always
  binds TCP on 127.0.0.1 with port 0 (OS-assigned).

  Returns `{:ok, endpoint}` where `endpoint` is the string to pass to
  `GRPC.Stub.connect/2`, e.g. `"127.0.0.1:PORT"`.

  Note: UDS via the Mint adapter is not supported in grpc 1.0 because
  `GRPC.Client.Adapters.Mint` does not pass the required `hostname:` option
  to `Mint.HTTP.connect/4` when the host is a `{:local, path}` tuple.
  TCP with port 0 is used instead.

  The server is started with `GRPC.Server.start/3` which returns
  `{:ok, _pid, port}` on success.
  """
  @spec start(Path.t()) :: {:ok, String.t()} | {:error, any()}
  def start(_sock_path) do
    case GRPC.Server.start(Impl, 0) do
      {:ok, _pid, port} ->
        {:ok, "127.0.0.1:#{port}"}

      {:error, {:already_started, _pid}} ->
        # Cowboy registers the listener under a fixed name derived from the
        # server module.  When multiple tests call start/1 sequentially the
        # server is already up; just retrieve the assigned port from Ranch.
        listener = inspect(Impl)
        port = :ranch.get_port(listener)
        {:ok, "127.0.0.1:#{port}"}

      {:error, _} = err ->
        err
    end
  end
end
