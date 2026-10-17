defmodule Embassy.Rails.Presence do
  @moduledoc """
  AnyCable-compatible presence tracking, cluster-wide via
  `Embassy.Rails.PresenceTracker` (a `Phoenix.Tracker`).

  One server per instance per node owns every entry joined on that node and
  monitors the joining connection processes. An explicit `leave/3` removes
  an entry at once. When a connection dies, its entries stay for the
  instance's `presence: [ttl_ms: ...]` (default 60s), so a client that
  reconnects within the TTL is never seen to leave.

  Several sessions may hold the same presence id on a stream (two tabs, or
  a reconnect overlapping a lingering entry): subscribers get one `join`
  when the first appears and one `leave` when the last is gone.

  If this server crashes, the tracker drops every entry it owned, so
  subscribers see a `leave` for each, and the server restarts empty.
  Connections are not told; their presence comes back when they next join.
  """

  use GenServer

  alias Embassy.Rails.Config

  @doc false
  def tracker_name(instance), do: Module.concat(instance, PresenceTracker)

  @doc false
  def server_name(instance), do: Module.concat(instance, Presence)

  @doc false
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: server_name(instance))
  end

  @doc """
  Join `stream` as presence `id` for session `sid`. The calling process is
  monitored; when it exits, its entries are removed after the TTL.

  A session's presence id on a stream is fixed: joining again with the same
  id updates its info, and with a different id returns
  `{:error, :id_mismatch}` (as anycable-go does).
  """
  @spec join(atom(), String.t(), String.t(), String.t(), term()) ::
          :ok | {:error, :id_mismatch | :disabled}
  def join(instance, stream, sid, id, info) do
    if enabled?(instance) do
      GenServer.call(server_name(instance), {:join, self(), stream, sid, id, info})
    else
      {:error, :disabled}
    end
  end

  @doc "Remove session `sid` from `stream` at once. A no-op when it is not present."
  @spec leave(atom(), String.t(), String.t()) :: :ok
  def leave(instance, stream, sid) do
    if enabled?(instance) do
      GenServer.call(server_name(instance), {:leave, stream, sid})
    else
      :ok
    end
  end

  @doc """
  The presence set of `stream` in anycable-go's shape:
  `%{"type" => "info", "total" => n, "records" => [%{"id" => id, "info" => info}]}`.
  `"records"` is left out when `return_records?` is false. A record's info
  is from that id's most recent join.
  """
  @spec info(atom(), String.t(), boolean()) :: {:ok, map()} | {:error, :disabled}
  def info(instance, stream, return_records?) do
    if enabled?(instance) do
      records = records(instance, stream)
      base = %{"type" => "info", "total" => length(records)}
      {:ok, if(return_records?, do: Map.put(base, "records", records), else: base)}
    else
      {:error, :disabled}
    end
  end

  defp records(instance, stream) do
    instance
    |> tracker_name()
    |> Phoenix.Tracker.list(stream)
    |> Enum.group_by(fn {_sid, meta} -> meta.id end, fn {_sid, meta} -> meta end)
    |> Enum.map(fn {id, metas} ->
      latest = Enum.max_by(metas, & &1.at)
      %{"id" => id, "info" => latest.info}
    end)
  end

  defp enabled?(instance), do: Config.snapshot(instance).presence != nil

  # --- server ---

  @impl true
  def init(opts) do
    {:ok,
     %{
       instance: Keyword.fetch!(opts, :instance),
       ttl_ms: Keyword.fetch!(opts, :ttl_ms),
       # sid => %{pid, ref, streams: %{stream => id}}
       sessions: %{},
       # monitor ref => sid
       refs: %{}
     }}
  end

  @impl true
  def handle_call({:join, pid, stream, sid, id, info}, _from, state) do
    session = Map.get(state.sessions, sid) || new_session(pid)
    meta = %{id: id, info: info, at: System.system_time(:microsecond)}
    tracker = tracker_name(state.instance)

    result =
      case session.streams do
        %{^stream => ^id} -> Phoenix.Tracker.update(tracker, self(), stream, sid, meta)
        %{^stream => _other} -> {:error, :id_mismatch}
        _ -> Phoenix.Tracker.track(tracker, self(), stream, sid, meta)
      end

    case result do
      {:ok, _ref} ->
        session = put_in(session.streams[stream], id)
        refs = Map.put(state.refs, session.ref, sid)
        {:reply, :ok, %{state | sessions: Map.put(state.sessions, sid, session), refs: refs}}

      {:error, _} = error ->
        {:reply, error, drop_if_empty(state, sid, session)}
    end
  end

  def handle_call({:leave, stream, sid}, _from, state) do
    case state.sessions do
      %{^sid => %{streams: %{^stream => _}} = session} ->
        :ok = Phoenix.Tracker.untrack(tracker_name(state.instance), self(), stream, sid)
        session = %{session | streams: Map.delete(session.streams, stream)}

        {:reply, :ok,
         drop_if_empty(%{state | sessions: Map.put(state.sessions, sid, session)}, sid, session)}

      _ ->
        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.refs, ref) do
      {nil, _} ->
        {:noreply, state}

      {sid, refs} ->
        Process.send_after(self(), {:expire, sid}, state.ttl_ms)
        {:noreply, %{state | refs: refs}}
    end
  end

  def handle_info({:expire, sid}, state) do
    case Map.pop(state.sessions, sid) do
      {nil, _} ->
        {:noreply, state}

      {session, sessions} ->
        tracker = tracker_name(state.instance)

        Enum.each(session.streams, fn {stream, _id} ->
          Phoenix.Tracker.untrack(tracker, self(), stream, sid)
        end)

        {:noreply, %{state | sessions: sessions}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp new_session(pid), do: %{pid: pid, ref: Process.monitor(pid), streams: %{}}

  # A session with no streams left is forgotten; its monitor goes with it.
  defp drop_if_empty(state, sid, %{streams: streams} = session) when map_size(streams) == 0 do
    Process.demonitor(session.ref, [:flush])

    %{
      state
      | sessions: Map.delete(state.sessions, sid),
        refs: Map.delete(state.refs, session.ref)
    }
  end

  defp drop_if_empty(state, _sid, _session), do: state
end
