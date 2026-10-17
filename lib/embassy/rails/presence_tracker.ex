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
  shard, so the session counts per presence id are kept here, in an ETS
  table owned by the `Embassy.Rails` supervisor. If the shard crashes, its
  entries go with it but the counts survive: the restarted shard sends a
  `leave` for every id it had announced, and ids still present on other
  nodes join again as it resyncs.

  The tracker's `down_period` is the instance's `ttl_ms` (at least 3s), so
  other nodes keep a dead node's entries for the TTL, as they keep a
  disconnected session's.

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
    # At least twice Phoenix.Tracker's 1500ms broadcast period.
    down_period = max(Keyword.fetch!(opts, :ttl_ms), 3_000)

    Phoenix.Tracker.start_link(__MODULE__, [instance: instance, pubsub: pubsub],
      name: Keyword.fetch!(opts, :name),
      pubsub_server: pubsub,
      pool_size: 1,
      down_period: down_period,
      permdown_period: max(1_200_000, down_period * 2)
    )
  end

  @doc false
  # Called from the Embassy.Rails supervisor's init, so the table outlives
  # a tracker shard.
  def new_counts_table(instance) do
    :ets.new(counts_table(instance), [:set, :public, :named_table])
  end

  defp counts_table(instance), do: Module.concat(instance, PresenceCounts)

  @impl Phoenix.Tracker
  def init(opts) do
    state = %{
      instance: Keyword.fetch!(opts, :instance),
      pubsub: Keyword.fetch!(opts, :pubsub),
      counts: counts_table(Keyword.fetch!(opts, :instance))
    }

    # Left over from a crashed shard: its entries are gone.
    for {{stream, id}, _count} <- :ets.tab2list(state.counts) do
      announce(state, stream, %{"type" => "leave", "id" => id})
    end

    :ets.delete_all_objects(state.counts)
    {:ok, state}
  end

  @impl Phoenix.Tracker
  def handle_diff(diff, state) do
    Enum.each(diff, fn {stream, {joins, leaves}} ->
      apply_stream_diff(state, stream, joins, leaves)
    end)

    {:ok, state}
  end

  defp apply_stream_diff(state, stream, joins, leaves) do
    deltas =
      Enum.reduce(leaves, %{}, fn {_sid, meta}, acc -> Map.update(acc, meta.id, -1, &(&1 - 1)) end)

    deltas =
      Enum.reduce(joins, deltas, fn {_sid, meta}, acc ->
        Map.update(acc, meta.id, 1, &(&1 + 1))
      end)

    join_info = Map.new(joins, fn {_sid, meta} -> {meta.id, meta.info} end)

    Enum.each(deltas, fn
      {_id, 0} ->
        :ok

      {id, delta} ->
        key = {stream, id}

        before =
          case :ets.lookup(state.counts, key) do
            [{^key, count}] -> count
            [] -> 0
          end

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
          do: :ets.delete(state.counts, key),
          else: :ets.insert(state.counts, {key, after_count})
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
