if Code.ensure_loaded?(GRPC.Stub) do
  defmodule Embassy.Rails.RPC.Grpc do
    @moduledoc """
    gRPC transport for the AnyCable RPC. Builds protobuf requests from the
    same `env`/`payload` maps the HTTP transport uses and normalizes
    protobuf responses back into the string-keyed maps
    `Embassy.Rails.Connection` reads, so the transport is transparent.
    """

    alias Anycable.RPC.Stub, as: AnycableStub
    alias Embassy.Rails.GrpcChannel

    # --- request builders ------------------------------------------------

    @doc false
    def build_connect_request(env) do
      %Anycable.ConnectionRequest{env: to_env(env)}
    end

    @doc false
    def build_command_request(payload) do
      %Anycable.CommandMessage{
        command: payload[:command] || "",
        identifier: payload[:identifier] || "",
        connection_identifiers: payload[:connection_identifiers] || "",
        data: payload[:data] || "",
        env: to_env(payload[:env] || %{})
      }
    end

    @doc false
    def build_disconnect_request(payload) do
      %Anycable.DisconnectRequest{
        identifiers: payload[:identifiers] || "",
        subscriptions: payload[:subscriptions] || [],
        env: to_env(payload[:env] || %{})
      }
    end

    defp to_env(env) do
      %Anycable.Env{
        url: env[:url] || "",
        headers: stringify(env[:headers]),
        cstate: stringify(env[:cstate]),
        istate: stringify(env[:istate])
      }
    end

    # proto map<string,string>: keys and values must be strings.
    defp stringify(nil), do: %{}

    defp stringify(map) when is_map(map) do
      Map.new(map, fn {k, v} -> {to_string(k), to_string(v)} end)
    end

    # --- response normalizers --------------------------------------------

    @doc false
    def normalize_connect(%Anycable.ConnectionResponse{} = resp) do
      %{
        "status" => status_int(resp.status),
        "identifiers" => resp.identifiers,
        "transmissions" => resp.transmissions || [],
        "env" => env_response(resp.env)
      }
    end

    @doc false
    def normalize_command(%Anycable.CommandResponse{} = resp) do
      %{
        "status" => status_int(resp.status),
        "disconnect" => resp.disconnect || false,
        "stop_streams" => resp.stop_streams || false,
        "streams" => resp.streams || [],
        "transmissions" => resp.transmissions || [],
        "stopped_streams" => resp.stopped_streams || [],
        "env" => env_response(resp.env),
        "presence" => presence_response(resp.presence)
      }
    end

    @doc false
    def normalize_disconnect(%Anycable.DisconnectResponse{} = resp) do
      %{"status" => status_int(resp.status)}
    end

    # The generated enum decodes to an atom; Connection matches the integer.
    defp status_int(status) when is_integer(status), do: status
    defp status_int(status) when is_atom(status), do: Anycable.Status.value(status)

    defp presence_response(nil), do: nil

    defp presence_response(%Anycable.PresenceResponse{} = presence) do
      info = if presence.info in [nil, ""], do: nil, else: presence.info
      %{"type" => presence.type, "id" => presence.id, "info" => info}
    end

    defp env_response(nil), do: %{"cstate" => %{}, "istate" => %{}}

    defp env_response(%Anycable.EnvResponse{} = env) do
      %{"cstate" => env.cstate || %{}, "istate" => env.istate || %{}}
    end

    # --- channel dispatch -------------------------------------------------

    @doc false
    def call(instance, op, request, opts \\ []) do
      case GrpcChannel.fetch_channel(instance) do
        {:ok, channel} ->
          channel |> do_call(op, request, opts) |> maybe_reconnect(instance, channel)

        :error ->
          {:error, :no_channel}
      end
    end

    defp maybe_reconnect({:error, reason} = err, instance, channel) do
      if connection_lost?(reason), do: GrpcChannel.reconnect(instance, channel)
      err
    end

    defp maybe_reconnect(ok, _instance, _channel), do: ok

    defp do_call(channel, :connect, request, opts) do
      case AnycableStub.connect(channel, request, opts) do
        {:ok, resp} -> {:ok, normalize_connect(resp)}
        {:error, reason} -> {:error, grpc_error(reason)}
      end
    end

    defp do_call(channel, :command, request, opts) do
      case AnycableStub.command(channel, request, opts) do
        {:ok, resp} -> {:ok, normalize_command(resp)}
        {:error, reason} -> {:error, grpc_error(reason)}
      end
    end

    defp do_call(channel, :disconnect, request, opts) do
      case AnycableStub.disconnect(channel, request, opts) do
        {:ok, resp} -> {:ok, normalize_disconnect(resp)}
        {:error, reason} -> {:error, grpc_error(reason)}
      end
    end

    # The connection, not the call, failed: the Mint connection process
    # stays closed after the server goes away, so only a new channel
    # recovers. A deadline or an application status is not a reason to
    # reconnect.
    @doc false
    def connection_lost?({:grpc, 14, _message}), do: true

    def connection_lost?({:grpc, 2, message}) when is_binary(message),
      do: String.contains?(message, "connection is closed")

    def connection_lost?({:grpc, _status, _message}), do: false
    def connection_lost?({:grpc, _other}), do: true

    defp grpc_error(%GRPC.RPCError{status: status, message: message}),
      do: {:grpc, status, message}

    defp grpc_error(other), do: {:grpc, other}
  end
else
  defmodule Embassy.Rails.RPC.Grpc do
    @moduledoc false
  end
end
