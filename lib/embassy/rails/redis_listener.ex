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
