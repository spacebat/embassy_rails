if Code.ensure_loaded?(GRPC.Stub) do
  defmodule Embassy.Rails.GrpcChannel do
    @moduledoc """
    Holds the live gRPC channel for one Embassy.Rails instance and
    publishes it to the instance Registry meta (`:grpc_channel`) for O(1)
    reads by `Embassy.Rails.RPC.Grpc`. Reconnects with capped backoff;
    a transient connect failure does not crash the instance.

    A reconnect closes the previous channel first, so connections are not
    leaked. Callers that saw a channel fail pass that channel to
    `reconnect/2`; a request naming a channel that has already been
    replaced is ignored, so a burst of failures opens one new connection,
    and a channel is replaced at most once a second. The connection
    process is monitored, so one that dies is replaced without waiting
    for a call to fail.

    Emits `[:embassy_rails, :rpc, :grpc, :connected]` (metadata
    `instance`, `endpoint`) each time a channel is published.
    """

    use GenServer

    require Logger

    @backoff_start 200
    @backoff_max 5_000
    # A channel is replaced at most this often after a successful connect,
    # so a server that accepts connections but fails every call (a proxy
    # in front of a dead RPC server answers UNAVAILABLE) is not reconnected
    # once per failed call.
    @min_channel_age_ms 1_000

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
    rescue
      ArgumentError -> :error
    end

    @doc """
    Replace `failed`, the channel a caller saw fail, with a new one. Does
    nothing if `failed` is no longer the current channel. With `:any`,
    reconnects unconditionally.
    """
    @spec reconnect(atom(), GRPC.Channel.t() | :any) :: :ok
    def reconnect(instance, failed \\ :any) do
      case whereis(instance) do
        nil -> :ok
        pid -> send(pid, {:reconnect, failed})
      end

      :ok
    end

    @doc false
    def whereis(instance), do: GenServer.whereis(via(instance))

    defp via(instance), do: {:via, Registry, {Embassy.Rails.registry_name(instance), __MODULE__}}

    @impl true
    def init(opts) do
      # So terminate/2 runs on shutdown and closes the channel.
      Process.flag(:trap_exit, true)

      state = %{
        instance: Keyword.fetch!(opts, :instance),
        endpoint: Keyword.fetch!(opts, :endpoint),
        secret: Keyword.fetch!(opts, :secret),
        channel: nil,
        monitor: nil,
        connected_at: nil,
        replace: nil,
        retry: nil,
        backoff: @backoff_start
      }

      {:ok, state, {:continue, :connect}}
    end

    @impl true
    def handle_continue(:connect, state), do: {:noreply, connect(state)}

    @impl true
    def handle_info(:retry, state), do: {:noreply, connect(%{state | retry: nil})}

    # Already disconnected, with a retry scheduled: that retry reconnects.
    def handle_info({:reconnect, _failed}, %{channel: nil, retry: retry} = state)
        when retry != nil,
        do: {:noreply, state}

    def handle_info({:reconnect, :any}, state), do: {:noreply, connect(state)}

    def handle_info({:reconnect, channel}, %{channel: channel} = state),
      do: {:noreply, replace(state)}

    def handle_info({:reconnect, _stale}, state), do: {:noreply, state}

    # The connection process died. grpc links it to its own connection
    # process, not to this one, so it is monitored here.
    def handle_info({:DOWN, ref, :process, _pid, _reason}, %{monitor: ref} = state),
      do: {:noreply, replace(%{state | monitor: nil})}

    # A replacement cancelled by a reconnect may already have been sent.
    def handle_info(:replace, %{replace: nil} = state), do: {:noreply, state}
    def handle_info(:replace, state), do: {:noreply, connect(%{state | replace: nil})}

    def handle_info(_msg, state), do: {:noreply, state}

    @impl true
    def terminate(_reason, state), do: close(state)

    # Replace the current channel now, or once it is old enough; requests
    # meanwhile share the one scheduled replacement.
    defp replace(%{replace: ref} = state) when ref != nil, do: state

    defp replace(state) do
      age = System.monotonic_time(:millisecond) - state.connected_at

      if age >= @min_channel_age_ms do
        connect(state)
      else
        %{state | replace: Process.send_after(self(), :replace, @min_channel_age_ms - age)}
      end
    end

    defp connect(state) do
      if state.replace, do: Process.cancel_timer(state.replace)
      state = close(%{state | replace: nil})

      # Bounded so an endpoint that drops SYNs does not block this process
      # (and its shutdown) for the default 20 s: the TCP connect gives up
      # at 2 s, the wait for the channel at 3 s.
      opts = [
        adapter: GRPC.Client.Adapters.Mint,
        adapter_opts: [transport_opts: [timeout: 2_000]],
        connect_timeout: 3_000,
        headers: [{"authorization", "Bearer " <> state.secret}]
      ]

      case GRPC.Stub.connect(state.endpoint, opts) do
        {:ok, channel} ->
          Registry.put_meta(Embassy.Rails.registry_name(state.instance), :grpc_channel, channel)

          :telemetry.execute([:embassy_rails, :rpc, :grpc, :connected], %{}, %{
            instance: state.instance,
            endpoint: state.endpoint
          })

          %{
            state
            | channel: channel,
              monitor: monitor(channel),
              connected_at: System.monotonic_time(:millisecond),
              backoff: @backoff_start
          }

        {:error, reason} ->
          Logger.warning(
            "Embassy.Rails.GrpcChannel(#{inspect(state.instance)}): connect failed: #{inspect(reason)}; retrying in #{state.backoff}ms"
          )

          retry = Process.send_after(self(), :retry, state.backoff)
          %{state | retry: retry, backoff: min(state.backoff * 2, @backoff_max)}
      end
    end

    # Unpublish first so no caller picks up a channel that is being closed.
    defp close(%{channel: nil} = state), do: state

    defp close(%{channel: channel} = state) do
      if state.monitor, do: Process.demonitor(state.monitor, [:flush])
      unpublish(state.instance)

      try do
        GRPC.Stub.disconnect(channel)
      catch
        :exit, _ -> :ok
      end

      %{state | channel: nil, monitor: nil}
    end

    # The config Registry is already gone when it is what restarted, and
    # the channel must still be closed.
    defp unpublish(instance) do
      Registry.put_meta(Embassy.Rails.registry_name(instance), :grpc_channel, nil)
    rescue
      ArgumentError -> :ok
    end

    defp monitor(%GRPC.Channel{adapter_payload: %{conn_pid: pid}}) when is_pid(pid),
      do: Process.monitor(pid)

    defp monitor(_channel), do: nil
  end
else
  defmodule Embassy.Rails.GrpcChannel do
    @moduledoc false
  end
end
