defmodule Embassy.Rails.RedisListener do
  @moduledoc """
  Subscribes to an AnyCable Redis broadcaster channel and fans
  publications out through `Embassy.Rails.Broadcast.dispatch/3`.

  Started by the `Embassy.Rails` supervisor only when the instance is
  configured with `broadcast: [source: :redis, ...]`. The AnyCable Redis
  broadcaster publishes JSON `{"stream", "data"}` envelopes to a single
  channel (default `"__anycable__"`); this listener decodes them and
  hands them to the same dispatch path the HTTP `BroadcastPlug` uses, so
  the broadcast source stays invisible to the WS handlers.
  """

  use GenServer
  require Logger

  alias Embassy.Rails.Broadcast

  @default_channel "__anycable__"

  @impl true
  def handle_info({:redix_pubsub, _conn, _ref, :message, %{payload: payload}}, state) do
    case decode_anycable(payload) do
      {:ok, stream, broadcast_payload} ->
        Broadcast.dispatch(state.instance, stream, broadcast_payload)

      :error ->
        Logger.warning("Embassy.Rails.RedisListener: malformed broadcast envelope")

        :telemetry.execute(
          [:embassy_rails, :broadcast, :redis, :exception],
          %{monotonic_time: System.monotonic_time()},
          %{instance: state.instance, kind: :bad_envelope}
        )
    end

    {:noreply, state}
  end

  def handle_info({:redix_pubsub, _conn, _ref, :subscribed, %{channel: channel}}, state) do
    Logger.info("Embassy.Rails.RedisListener: subscribed to #{channel}")

    :telemetry.execute(
      [:embassy_rails, :broadcast, :redis, :connected],
      %{monotonic_time: System.monotonic_time()},
      %{instance: state.instance, channel: channel}
    )

    {:noreply, state}
  end

  def handle_info({:redix_pubsub, _conn, _ref, :disconnected, _props}, state) do
    Logger.warning("Embassy.Rails.RedisListener: disconnected from Redis")

    :telemetry.execute(
      [:embassy_rails, :broadcast, :redis, :disconnected],
      %{monotonic_time: System.monotonic_time()},
      %{instance: state.instance}
    )

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @doc """
  Decode an AnyCable Redis broadcaster envelope.

  Returns `{:ok, stream, %{data: ..., exclude_socket: ...}}` for an
  envelope with a binary `"stream"`, or `:error` for malformed JSON, a
  missing `"stream"`, or a non-binary `"stream"`.
  """
  @spec decode_anycable(binary()) ::
          {:ok, String.t(), %{data: term(), exclude_socket: term()}} | :error
  def decode_anycable(payload) when is_binary(payload) do
    case Jason.decode(payload) do
      {:ok, %{"stream" => stream} = env} when is_binary(stream) ->
        {:ok, stream,
         %{data: env["data"], exclude_socket: get_in(env, ["meta", "exclude_socket"])}}

      _ ->
        :error
    end
  end
end
