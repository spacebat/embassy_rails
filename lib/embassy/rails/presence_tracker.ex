defmodule Embassy.Rails.PresenceTracker do
  @moduledoc """
  `Phoenix.Tracker` behind `Embassy.Rails.Presence`. Replicates presence
  entries across the cluster and turns diffs into AnyCable join and leave
  events.

  Entries are keyed by session id with meta `%{id: presence_id, info: info}`,
  so several sessions can hold one presence id. A `join` event goes out
  when a presence id gains its first session on a stream, and a `leave`
  when it loses its last. Events are local-broadcast as
  `{:cable_presence, stream, event}` on `Embassy.Rails.Broadcast.topic/1`:
  every node's tracker sees every diff, so each node informs its own
  subscribers.

  `Phoenix.Tracker` calls `handle_diff/2` before it commits the change, and
  reading the tracker from inside the callback would call into the same
  shard, so the session counts per presence id are kept here.

  Telemetry `[:embassy_rails, :presence, :join | :leave]` (measurement
  `count: 1`, metadata `instance`, `stream`, `id`) fires on every node for
  every event.
  """

  use Phoenix.Tracker

  alias Embassy.Rails.Broadcast

  @doc false
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    pubsub = Keyword.fetch!(opts, :pubsub)

    Phoenix.Tracker.start_link(__MODULE__, [instance: instance, pubsub: pubsub],
      name: Keyword.fetch!(opts, :name),
      pubsub_server: pubsub,
      pool_size: 1
    )
  end

  @impl Phoenix.Tracker
  def init(opts) do
    {:ok,
     %{
       instance: Keyword.fetch!(opts, :instance),
       pubsub: Keyword.fetch!(opts, :pubsub),
       counts: %{}
     }}
  end

  @impl Phoenix.Tracker
  def handle_diff(diff, state) do
    counts =
      Enum.reduce(diff, state.counts, fn {stream, {joins, leaves}}, counts ->
        apply_stream_diff(state, stream, joins, leaves, counts)
      end)

    {:ok, %{state | counts: counts}}
  end

  defp apply_stream_diff(state, stream, joins, leaves, counts) do
    deltas =
      Enum.reduce(leaves, %{}, fn {_sid, meta}, acc -> Map.update(acc, meta.id, -1, &(&1 - 1)) end)

    deltas =
      Enum.reduce(joins, deltas, fn {_sid, meta}, acc ->
        Map.update(acc, meta.id, 1, &(&1 + 1))
      end)

    join_info = Map.new(joins, fn {_sid, meta} -> {meta.id, meta.info} end)

    Enum.reduce(deltas, counts, fn
      {_id, 0}, counts ->
        counts

      {id, delta}, counts ->
        before = Map.get(counts, {stream, id}, 0)
        after_count = max(before + delta, 0)

        cond do
          before == 0 and after_count > 0 ->
            announce(state, stream, join_event(id, join_info[id]))

          before > 0 and after_count == 0 ->
            announce(state, stream, %{"type" => "leave", "id" => id})

          true ->
            :ok
        end

        if after_count == 0,
          do: Map.delete(counts, {stream, id}),
          else: Map.put(counts, {stream, id}, after_count)
    end)
  end

  defp event_name("join"), do: :join
  defp event_name("leave"), do: :leave

  defp join_event(id, nil), do: %{"type" => "join", "id" => id}
  defp join_event(id, info), do: %{"type" => "join", "id" => id, "info" => info}

  defp announce(state, stream, %{"type" => type, "id" => id} = event) do
    Phoenix.PubSub.local_broadcast(
      state.pubsub,
      Broadcast.topic(stream),
      {:cable_presence, stream, event}
    )

    :telemetry.execute(
      [:embassy_rails, :presence, event_name(type)],
      %{count: 1},
      %{instance: state.instance, stream: stream, id: id}
    )
  end
end
