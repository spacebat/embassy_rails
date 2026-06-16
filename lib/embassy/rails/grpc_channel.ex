if Code.ensure_loaded?(GRPC.Stub) do
  defmodule Embassy.Rails.GrpcChannel do
    @moduledoc """
    Holds the live gRPC channel for one Embassy.Rails instance and
    publishes it to the instance Registry meta (`:grpc_channel`) for O(1)
    reads by `Embassy.Rails.RPC.Grpc`. Reconnects with capped backoff;
    a transient connect failure does not crash the instance.
    """

    use GenServer

    require Logger

    @backoff_start 200
    @backoff_max 5_000

    @doc false
    def start_link(opts) do
      instance = Keyword.fetch!(opts, :instance)
      GenServer.start_link(__MODULE__, opts, name: via(instance))
    end

    @doc """
    Return the current channel for `instance`, or `:error` if none is
    published yet (not connected). O(1) Registry meta read.
    """
    @spec fetch_channel(atom()) :: {:ok, GRPC.Channel.t()} | :error
    def fetch_channel(instance) do
      case Registry.meta(Embassy.Rails.registry_name(instance), :grpc_channel) do
        {:ok, %GRPC.Channel{} = channel} -> {:ok, channel}
        _ -> :error
      end
    end

    defp via(instance), do: {:via, Registry, {Embassy.Rails.registry_name(instance), __MODULE__}}

    @impl true
    def init(opts) do
      state = %{
        instance: Keyword.fetch!(opts, :instance),
        endpoint: Keyword.fetch!(opts, :endpoint),
        secret: Keyword.fetch!(opts, :secret),
        channel: nil,
        backoff: @backoff_start
      }

      {:ok, state, {:continue, :connect}}
    end

    @impl true
    def handle_continue(:connect, state), do: {:noreply, connect(state)}

    @impl true
    def handle_info(:reconnect, state), do: {:noreply, connect(state)}

    @impl true
    def handle_info(_msg, state), do: {:noreply, state}

    defp connect(state) do
      opts = [
        adapter: GRPC.Client.Adapters.Mint,
        headers: [{"authorization", "Bearer " <> state.secret}]
      ]

      case GRPC.Stub.connect(state.endpoint, opts) do
        {:ok, channel} ->
          Registry.put_meta(Embassy.Rails.registry_name(state.instance), :grpc_channel, channel)
          %{state | channel: channel, backoff: @backoff_start}

        {:error, reason} ->
          Registry.put_meta(Embassy.Rails.registry_name(state.instance), :grpc_channel, nil)

          Logger.warning(
            "Embassy.Rails.GrpcChannel(#{inspect(state.instance)}): connect failed: #{inspect(reason)}; retrying in #{state.backoff}ms"
          )

          Process.send_after(self(), :reconnect, state.backoff)
          %{state | channel: nil, backoff: min(state.backoff * 2, @backoff_max)}
      end
    end
  end
else
  defmodule Embassy.Rails.GrpcChannel do
    @moduledoc false
  end
end
