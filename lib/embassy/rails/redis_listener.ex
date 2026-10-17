defmodule Embassy.Rails.RedisListener do
  @moduledoc """
  Subscribes to an AnyCable Redis broadcaster channel and fans
  publications out through `Embassy.Rails.Broadcast.dispatch/4`.

  Started by the `Embassy.Rails` supervisor only when the instance is
  configured with `broadcast: [source: :redis, ...]`. The AnyCable Redis
  broadcaster publishes JSON `{"stream", "data"}` envelopes (or batches
  of them) to a single channel (default `"__anycable__"`); this listener
  decodes them with `Embassy.Rails.BroadcastEnvelope` and
  hands them to the same dispatch path the HTTP `BroadcastPlug` uses, so
  the broadcast source stays invisible to the WS handlers.
  """

  use GenServer
  require Logger

  alias Embassy.Rails.BroadcastEnvelope

  @default_channel "__anycable__"

  @doc false
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    channel = Keyword.get(opts, :channel) || @default_channel
    redis = Keyword.fetch!(opts, :redis)

    {:ok, conn} = Redix.PubSub.start_link(redis)
    {:ok, ref} = Redix.PubSub.subscribe(conn, channel, self())

    {:ok, %{instance: instance, channel: channel, conn: conn, ref: ref, status: :initial}}
  end

  @impl true
  def handle_info({:redix_pubsub, _conn, _ref, :message, %{payload: payload}}, state) do
    case BroadcastEnvelope.decode(payload) do
      {:ok, items} ->
        BroadcastEnvelope.dispatch_items(state.instance, items, :local)

      {:error, _} ->
        Logger.warning("Embassy.Rails.RedisListener: malformed broadcast envelope")

        :telemetry.execute(
          [:embassy_rails, :broadcast, :redis, :exception],
          %{monotonic_time: System.monotonic_time()},
          %{instance: state.instance, kind: :bad_envelope}
        )
    end

    {:noreply, state}
  end

  def handle_info(
        {:redix_pubsub, _conn, _ref, :subscribed, %{channel: channel}},
        %{status: :disconnected} = state
      ) do
    Logger.info("Embassy.Rails.RedisListener: reconnected to #{channel}")

    :telemetry.execute(
      [:embassy_rails, :broadcast, :redis, :reconnected],
      %{monotonic_time: System.monotonic_time()},
      %{instance: state.instance, channel: channel}
    )

    {:noreply, %{state | status: :connected}}
  end

  def handle_info({:redix_pubsub, _conn, _ref, :subscribed, %{channel: channel}}, state) do
    Logger.info("Embassy.Rails.RedisListener: subscribed to #{channel}")

    :telemetry.execute(
      [:embassy_rails, :broadcast, :redis, :connected],
      %{monotonic_time: System.monotonic_time()},
      %{instance: state.instance, channel: channel}
    )

    {:noreply, %{state | status: :connected}}
  end

  def handle_info({:redix_pubsub, _conn, _ref, :disconnected, props}, state) do
    suffix =
      case Map.get(props, :reconnect_timeout) do
        nil -> ""
        ms -> "; reconnecting in #{ms}ms"
      end

    Logger.warning("Embassy.Rails.RedisListener: disconnected from Redis#{suffix}")

    :telemetry.execute(
      [:embassy_rails, :broadcast, :redis, :disconnected],
      %{monotonic_time: System.monotonic_time()},
      %{instance: state.instance}
    )

    {:noreply, %{state | status: :disconnected}}
  end

  def handle_info(_msg, state), do: {:noreply, state}
end
