defmodule Embassy.Rails.Broadcast do
  @moduledoc """
  PubSub fan-out for AnyCable broadcasts entering the BEAM.

  Both `Embassy.Rails.BroadcastPlug` (HTTP source) and any future Redis
  listener (v0.2+) call `dispatch/3`. The single dispatch path makes
  the source mode invisible to the WS handlers.
  """

  alias Embassy.Rails.Config

  @topic_prefix "embassy_rails:"

  @doc "Topic-naming helper. `topic_prefix() <> stream_id`."
  @spec topic(String.t()) :: String.t()
  def topic(stream) when is_binary(stream), do: @topic_prefix <> stream

  @doc "The constant topic prefix used by `topic/1`."
  @spec topic_prefix() :: String.t()
  def topic_prefix, do: @topic_prefix

  @doc """
  Fan an entry out over `Phoenix.PubSub` to every WS handler subscribed
  to this stream on the local node.

  The message shape is `{:cable_broadcast, stream, payload}` so
  `Embassy.Rails.Connection.handle_info/2` can pattern-match.
  """
  @spec dispatch(atom(), String.t(), map()) :: :ok
  def dispatch(instance, stream, payload) when is_atom(instance) and is_binary(stream) do
    snap = Config.snapshot(instance)

    Phoenix.PubSub.local_broadcast(
      snap.pubsub,
      topic(stream),
      {:cable_broadcast, stream, payload}
    )

    :telemetry.execute(
      [:embassy_rails, :broadcast, :received],
      %{monotonic_time: System.monotonic_time(), payload_bytes: payload_bytes(payload)},
      %{instance: instance, stream: stream}
    )

    :ok
  end

  defp payload_bytes(%{data: data}) when is_binary(data), do: byte_size(data)
  defp payload_bytes(_), do: 0
end
