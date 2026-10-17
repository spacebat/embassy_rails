defmodule Embassy.Rails.Drain do
  @moduledoc """
  Graceful WebSocket drain. On shutdown it marks the instance as draining
  (so `Embassy.Rails.Health.ready?/1` turns false) and asks every
  registered connection to close with a reconnecting `disconnect`
  (`{"type":"disconnect","reason":"server_restart","reconnect":true}`),
  spread over `:window_ms` (default 60_000) in `:batches` (default 20), so
  clients do not all reconnect at once.

  Start it in your application tree **after** the Endpoint, so that it
  terminates first:

      {Embassy.Rails.Drain, instance: MyApp.Embassy.Rails, window_ms: 30_000}

  Its child spec gives it a shutdown longer than the window.
  """

  use GenServer

  @default_window_ms 60_000
  @default_batches 20

  @doc false
  def child_spec(opts) do
    window = Keyword.get(opts, :window_ms, @default_window_ms)

    %{
      id: {__MODULE__, Keyword.fetch!(opts, :instance)},
      start: {__MODULE__, :start_link, [opts]},
      shutdown: window + 5_000
    }
  end

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc "Whether `instance` has begun draining."
  @spec draining?(atom()) :: boolean()
  def draining?(instance), do: :persistent_term.get({__MODULE__, instance}, false)

  @doc false
  # Called when an Embassy.Rails instance starts, so a restarted instance
  # never inherits a previous drain.
  @spec reset(atom()) :: :ok
  def reset(instance) do
    if draining?(instance), do: :persistent_term.put({__MODULE__, instance}, false)
    :ok
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    instance = Keyword.fetch!(opts, :instance)
    :persistent_term.put({__MODULE__, instance}, false)

    {:ok,
     %{
       instance: instance,
       window_ms: Keyword.get(opts, :window_ms, @default_window_ms),
       batches: Keyword.get(opts, :batches, @default_batches)
     }}
  end

  @impl true
  def terminate(_reason, state) do
    :persistent_term.put({__MODULE__, state.instance}, true)

    pids =
      state.instance
      |> Embassy.Rails.connections_name()
      |> Registry.lookup(:conn)
      |> Enum.map(fn {pid, _} -> pid end)

    batch_size = max(1, ceil(length(pids) / state.batches))
    pause = div(state.window_ms, state.batches)

    pids
    |> Enum.chunk_every(batch_size)
    |> Enum.with_index()
    |> Enum.each(fn {batch, i} ->
      if i > 0, do: Process.sleep(pause)
      Enum.each(batch, &send(&1, :embassy_rails_drain))
    end)

    :ok
  end
end
