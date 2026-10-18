defmodule Embassy.Rails.RedisStreamListener do
  @moduledoc """
  Consumes AnyCable's Redis Streams broadcaster (`redisx`) and fans
  entries out through `Embassy.Rails.Broadcast.dispatch/4`.

  Started by the `Embassy.Rails` supervisor when the instance is
  configured with `broadcast: [source: :redisx, ...]`. Rails must use the
  matching adapter: `broadcast_adapter: redisx` in `config/anycable.yml`,
  or `ANYCABLE_BROADCAST_ADAPTER=redisx`.

  The defaults mirror anycable-go, so this is a drop-in replacement for
  it: stream `"__anycable__"`, consumer group `"bx"`, a 2000 ms block,
  and the entry field `"payload"`. The listener:

    * creates the group at `$` with `MKSTREAM` (an existing group is
      fine). If the group disappears (`NOGROUP`, or `UNBLOCKED` when the
      stream key is deleted) it is recreated at `0`, so entries added
      while it was gone are still delivered
    * reads with `XREADGROUP ... BLOCK`
    * every block period, reclaims entries that another consumer left
      pending for longer than the block period (`XAUTOCLAIM`)
    * after dispatching, `XACK`s and `XDEL`s every entry
    * removes its consumer from the group (`XGROUP DELCONSUMER`) on
      shutdown

  Delivery is at-least-once: an entry is dispatched before it is acked,
  so a crash in between means another consumer reclaims and dispatches
  it again. Entries without a `payload` field, or whose payload cannot be
  decoded, are acked and deleted with `:exception` telemetry rather than
  left to be reclaimed forever (anycable-go leaves them pending).

  Each entry reaches exactly one consumer in the group, so this source
  dispatches with `:cluster` scope. A multi-node deployment therefore
  needs a clustered `Phoenix.PubSub` (the default PG2 adapter over
  distributed Erlang, or any cross-node adapter); without one, a
  broadcast reaches only the subscribers on whichever node read it.

  Every entry is `XDEL`ed once read, as anycable-go does, so a stream must
  have a single reader. Another consumer group on the same stream, or an
  anycable-go still reading it, loses entries: cut a stream over from
  anycable-go in one step.

  Requires Redis 6.2 or newer (`XAUTOCLAIM`). The Ruby producer does not
  trim the stream, and entries are only deleted once consumed, so the
  stream grows while no consumer is running.

  ## Options

    * `:instance` (required) - the `Embassy.Rails` instance name
    * `:redis` (required) - a Redis URL or `Redix.start_link/1` options
    * `:stream` - default `"__anycable__"`
    * `:group` - default `"bx"`
    * `:block_ms` - default `2000`
    * `:consumer` - default `"<node>-<6 random chars>"`

  ## Telemetry

  Events under `[:embassy_rails, :broadcast, :redisx, _]`, with metadata
  `instance`, `stream`, `group` and `consumer`:

    * `:connected` - the group is ready, the first time
    * `:disconnected` - the first failure of a streak, plus `reason`
    * `:reconnected` - the first success after a failure
    * `:claimed` - measurement `count`, when entries were reclaimed
    * `:exception` - plus `kind` (`:bad_envelope | :missing_payload`)
      and the entry `id`
  """

  use GenServer
  require Logger

  alias Embassy.Rails.{Broadcast, BroadcastEnvelope, Health}

  @default_stream "__anycable__"
  @default_group "bx"
  @default_block_ms 2_000
  @read_count 100
  @max_backoff_ms 30_000

  @doc false
  # A shutdown waits for a blocked XREADGROUP to return before terminate/2
  # can remove the consumer, so it must outlast the block.
  def child_spec(opts) do
    block_ms = Keyword.get(opts, :block_ms) || @default_block_ms
    Map.put(super(opts), :shutdown, block_ms + 5_000)
  end

  @doc false
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    redis = Keyword.fetch!(opts, :redis)
    Health.put_source_ready(Keyword.fetch!(opts, :instance), false)
    {:ok, conn} = start_redix(redis)

    state = %{
      instance: Keyword.fetch!(opts, :instance),
      redis: redis,
      conn: conn,
      stream: Keyword.get(opts, :stream) || @default_stream,
      group: Keyword.get(opts, :group) || @default_group,
      block_ms: Keyword.get(opts, :block_ms) || @default_block_ms,
      consumer: Keyword.get(opts, :consumer) || default_consumer(),
      status: :initial,
      failures: 0,
      last_claimed_at: nil
    }

    send(self(), :poll)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    with {:ok, state} <- ensure_group(state),
         {:ok, state} <- maybe_claim(state),
         {:ok, state} <- read(state) do
      send(self(), :poll)
      {:noreply, state}
    else
      {:error, error, state} -> {:noreply, handle_error(error, state)}
    end
  end

  def handle_info({:EXIT, conn, reason}, %{conn: conn} = state) do
    {:ok, new_conn} = start_redix(state.redis)
    {:noreply, disconnected(%{state | conn: new_conn}, {:redix_exited, reason})}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    _ = command(state, ["XGROUP", "DELCONSUMER", state.stream, state.group, state.consumer])
    _ = catch_exit(fn -> Redix.stop(state.conn) end)
    :ok
  end

  @doc """
  Parse an `XREADGROUP` reply into `{id, fields}` pairs. `nil` (a block
  timeout) gives no entries.
  """
  @spec parse_read_reply(term()) :: [{String.t(), %{String.t() => String.t()}}]
  def parse_read_reply(nil), do: []

  def parse_read_reply(streams) when is_list(streams) do
    Enum.flat_map(streams, fn [_stream, entries] -> parse_entries(entries) end)
  end

  @doc """
  Parse an `XAUTOCLAIM` reply, in either the Redis 6.2 shape
  (`[cursor, entries]`) or the 7+ shape (`[cursor, entries, deleted]`).
  Entries deleted while pending come back as `nil` on 6.2 and are dropped.
  """
  @spec parse_autoclaim_reply(term()) :: [{String.t(), %{String.t() => String.t()}}]
  def parse_autoclaim_reply([_cursor, entries | _]), do: parse_entries(entries)

  defp parse_entries(entries) do
    for [id, fields] <- entries, do: {id, fields_map(fields)}
  end

  defp fields_map(nil), do: %{}

  defp fields_map(fields) do
    fields |> Enum.chunk_every(2) |> Map.new(fn [k, v] -> {k, v} end)
  end

  # --- loop steps ---

  defp ensure_group(%{status: :connected} = state), do: {:ok, state}

  defp ensure_group(state) do
    case command(state, [
           "XGROUP",
           "CREATE",
           state.stream,
           state.group,
           group_start(state),
           "MKSTREAM"
         ]) do
      {:ok, _} -> {:ok, connected(state)}
      {:error, %Redix.Error{message: "BUSYGROUP" <> _}} -> {:ok, connected(state)}
      {:error, error} -> {:error, error, state}
    end
  end

  # A new group starts at `$`, as in anycable-go. A lost group restarts at
  # `0`: consumed entries are deleted, so all that is left is undelivered,
  # including what was added while the group was gone.
  defp group_start(%{status: :initial}), do: "$"
  defp group_start(_state), do: "0"

  defp maybe_claim(state) do
    now = System.monotonic_time(:millisecond)

    if state.last_claimed_at && now - state.last_claimed_at < state.block_ms do
      {:ok, state}
    else
      claim(%{state | last_claimed_at: now})
    end
  end

  defp claim(state) do
    cmd = [
      "XAUTOCLAIM",
      state.stream,
      state.group,
      state.consumer,
      to_string(state.block_ms),
      "0-0",
      "COUNT",
      to_string(@read_count)
    ]

    case command(state, cmd) do
      {:ok, reply} ->
        entries = parse_autoclaim_reply(reply)

        if entries != [] do
          emit(state, :claimed, %{count: length(entries)}, %{})
        end

        handle_entries(entries, state)

      {:error, error} ->
        {:error, error, state}
    end
  end

  defp read(state) do
    cmd = [
      "XREADGROUP",
      "GROUP",
      state.group,
      state.consumer,
      "COUNT",
      to_string(@read_count),
      "BLOCK",
      to_string(state.block_ms),
      "STREAMS",
      state.stream,
      ">"
    ]

    case command(state, cmd, state.block_ms + 1_000) do
      {:ok, reply} -> reply |> parse_read_reply() |> handle_entries(state)
      {:error, error} -> {:error, error, state}
    end
  end

  defp handle_entries([], state), do: {:ok, state}

  defp handle_entries(entries, state) do
    Enum.each(entries, &handle_entry(&1, state))
    ids = Enum.map(entries, &elem(&1, 0))

    pipeline = [
      ["XACK", state.stream, state.group | ids],
      ["XDEL", state.stream | ids]
    ]

    case run(state, fn -> Redix.pipeline(state.conn, pipeline) end) do
      {:ok, _} -> {:ok, state}
      {:error, error} -> {:error, error, state}
    end
  end

  defp handle_entry({id, %{"payload" => payload}}, state) do
    case BroadcastEnvelope.decode(payload) do
      {:ok, items} ->
        BroadcastEnvelope.dispatch_items(state.instance, items, Broadcast.scope(:redisx))

      {:error, _} ->
        Logger.warning("Embassy.Rails.RedisStreamListener: malformed broadcast envelope #{id}")
        emit(state, :exception, %{}, %{kind: :bad_envelope, id: id})
    end
  end

  defp handle_entry({id, _fields}, state) do
    Logger.warning("Embassy.Rails.RedisStreamListener: entry #{id} has no payload field")
    emit(state, :exception, %{}, %{kind: :missing_payload, id: id})
  end

  # --- errors and status ---

  # The group vanished (XGROUP DESTROY), or the stream key was deleted
  # under a blocked read: recreate the group and poll again at once.
  defp handle_error(%Redix.Error{message: "NOGROUP" <> _}, state), do: regroup(state)
  defp handle_error(%Redix.Error{message: "UNBLOCKED" <> _}, state), do: regroup(state)

  defp handle_error(error, state) do
    state = disconnected(state, error)
    Process.send_after(self(), :poll, backoff_ms(state.failures))
    state
  end

  defp regroup(state) do
    send(self(), :poll)
    %{state | status: :regroup}
  end

  defp connected(%{status: :initial} = state) do
    Health.put_source_ready(state.instance, true)
    emit(state, :connected, %{}, %{})
    %{state | status: :connected, failures: 0}
  end

  defp connected(%{status: :disconnected} = state) do
    Logger.info("Embassy.Rails.RedisStreamListener: reconnected to #{state.stream}")
    Health.put_source_ready(state.instance, true)
    emit(state, :reconnected, %{}, %{})
    %{state | status: :connected, failures: 0}
  end

  defp connected(state), do: %{state | status: :connected, failures: 0}

  defp disconnected(%{status: :disconnected} = state, _reason) do
    %{state | failures: state.failures + 1}
  end

  defp disconnected(state, reason) do
    Logger.warning("Embassy.Rails.RedisStreamListener: Redis unavailable: #{inspect(reason)}")
    Health.put_source_ready(state.instance, false)
    emit(state, :disconnected, %{}, %{reason: reason})
    %{state | status: :disconnected, failures: 1}
  end

  defp backoff_ms(failures) do
    min(1_000 * Integer.pow(2, max(failures - 1, 0)), @max_backoff_ms)
  end

  # --- helpers ---

  defp start_redix(redis) when is_binary(redis), do: Redix.start_link(redis)
  defp start_redix(redis) when is_list(redis), do: Redix.start_link(redis)

  defp command(state, cmd, timeout \\ 5_000) do
    run(state, fn -> Redix.command(state.conn, cmd, timeout: timeout) end)
  end

  # Redix exits the caller when its process dies during a call; the
  # `:EXIT` message that follows restarts it.
  defp run(_state, fun) do
    fun.()
  catch
    :exit, reason -> {:error, %Redix.ConnectionError{reason: {:exited, reason}}}
  end

  defp catch_exit(fun) do
    fun.()
  catch
    :exit, _ -> :ok
  end

  defp emit(state, event, measurements, extra) do
    :telemetry.execute(
      [:embassy_rails, :broadcast, :redisx, event],
      Map.put(measurements, :monotonic_time, System.monotonic_time()),
      Map.merge(
        %{
          instance: state.instance,
          stream: state.stream,
          group: state.group,
          consumer: state.consumer
        },
        extra
      )
    )
  end

  defp default_consumer do
    suffix = 6 |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)
    "#{node()}-#{binary_part(suffix, 0, 6)}"
  end
end
