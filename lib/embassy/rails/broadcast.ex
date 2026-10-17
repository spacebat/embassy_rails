defmodule Embassy.Rails.Broadcast do
  @moduledoc """
  PubSub fan-out for AnyCable broadcasts entering the BEAM.

  Every broadcast source (`Embassy.Rails.BroadcastPlug`,
  `Embassy.Rails.RedisListener`) calls `dispatch/4`. The single dispatch
  path makes the source mode invisible to the WS handlers.
  """

  alias Embassy.Rails.Config

  @topic_prefix "embassy_rails:"

  @doc "Topic-naming helper. `topic_prefix() <> stream_id`."
  @spec topic(String.t()) :: String.t()
  def topic(stream) when is_binary(stream), do: @topic_prefix <> stream

  @doc "The constant topic prefix used by `topic/1`."
  @spec topic_prefix() :: String.t()
  def topic_prefix, do: @topic_prefix

  @typedoc """
  Where a dispatch reaches. `:local` reaches subscribers on this node
  only. `:cluster` reaches subscribers on every node of a clustered
  PubSub.
  """
  @type scope :: :local | :cluster

  @doc """
  Delivery scope for a broadcast source.

  `:redis` (Pub/Sub) delivers every message to every node, so it stays
  `:local`. `:http` (Rails posts to one load-balanced node) and `:redisx`
  (each stream entry is read by one node) deliver each message to a
  single node, so they dispatch cluster-wide.
  """
  @spec scope(atom()) :: scope()
  def scope(:redis), do: :local
  def scope(_source), do: :cluster

  @doc """
  Fan an entry out over `Phoenix.PubSub` to every WS handler subscribed
  to this stream, on the local node (`:local`) or across the cluster
  (`:cluster`). Without a scope, the instance's broadcast source decides
  (see `scope/1`).

  The message shape is `{:cable_broadcast, stream, payload}` so
  `Embassy.Rails.Connection` WS handlers can pattern-match in their
  `handle_info` callback.
  """
  @spec dispatch(atom(), String.t(), map()) :: :ok
  def dispatch(instance, stream, payload) when is_atom(instance) and is_binary(stream) do
    source = instance |> Config.snapshot() |> Map.fetch!(:broadcast) |> Keyword.get(:source)
    dispatch(instance, stream, payload, scope(source))
  end

  @spec dispatch(atom(), String.t(), map(), scope()) :: :ok
  def dispatch(instance, stream, payload, scope)
      when is_atom(instance) and is_binary(stream) and scope in [:local, :cluster] do
    snap = Config.snapshot(instance)
    message = {:cable_broadcast, stream, payload}

    case scope do
      :local -> Phoenix.PubSub.local_broadcast(snap.pubsub, topic(stream), message)
      :cluster -> Phoenix.PubSub.broadcast(snap.pubsub, topic(stream), message)
    end

    :telemetry.execute(
      [:embassy_rails, :broadcast, :received],
      %{monotonic_time: System.monotonic_time(), payload_bytes: payload_bytes(payload)},
      %{instance: instance, stream: stream, scope: scope}
    )

    :ok
  end

  defp payload_bytes(%{data: data}) when is_binary(data), do: byte_size(data)
  defp payload_bytes(_), do: 0
end
