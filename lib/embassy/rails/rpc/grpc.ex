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
        "env" => env_response(resp.env)
      }
    end

    @doc false
    def normalize_disconnect(%Anycable.DisconnectResponse{} = resp) do
      %{"status" => status_int(resp.status)}
    end

    # The generated enum decodes to an atom; Connection matches the integer.
    defp status_int(status) when is_integer(status), do: status
    defp status_int(status) when is_atom(status), do: Anycable.Status.value(status)

    defp env_response(nil), do: %{"cstate" => %{}, "istate" => %{}}

    defp env_response(%Anycable.EnvResponse{} = env) do
      %{"cstate" => env.cstate || %{}, "istate" => env.istate || %{}}
    end

    # --- channel dispatch -------------------------------------------------

    @doc false
    def call(instance, op, request) do
      case GrpcChannel.fetch_channel(instance) do
        {:ok, channel} ->
          case do_call(channel, op, request) do
            {:error, {:grpc, _}} = err ->
              GrpcChannel.reconnect(instance)
              err

            other ->
              other
          end

        :error ->
          {:error, :no_channel}
      end
    end

    defp do_call(channel, :connect, request) do
      case AnycableStub.connect(channel, request) do
        {:ok, resp} -> {:ok, normalize_connect(resp)}
        {:error, reason} -> {:error, grpc_error(reason)}
      end
    end

    defp do_call(channel, :command, request) do
      case AnycableStub.command(channel, request) do
        {:ok, resp} -> {:ok, normalize_command(resp)}
        {:error, reason} -> {:error, grpc_error(reason)}
      end
    end

    defp do_call(channel, :disconnect, request) do
      case AnycableStub.disconnect(channel, request) do
        {:ok, resp} -> {:ok, normalize_disconnect(resp)}
        {:error, reason} -> {:error, grpc_error(reason)}
      end
    end

    defp grpc_error(%GRPC.RPCError{status: status, message: message}),
      do: {:grpc, status, message}

    defp grpc_error(other), do: {:grpc, other}
  end
else
  defmodule Embassy.Rails.RPC.Grpc do
    @moduledoc false
  end
end
