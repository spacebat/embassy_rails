defmodule Embassy.Rails.BroadcastEnvelope do
  @moduledoc """
  Decoder for AnyCable broadcast payloads, shared by every broadcast
  source.

  AnyCable's Ruby broadcast adapters send one of three JSON shapes:

    * a single envelope, `{"stream": ..., "data": ..., "meta": {...}}`
    * a batch (from `AnyCable.broadcast_adapter.batching`), a JSON array
      of envelopes
    * a command, `{"command": ..., "payload": {...}}`, such as the remote
      `"disconnect"` sent by `ActionCable.server.remote_connections`
  """

  require Logger

  alias Embassy.Rails.Broadcast

  @type item ::
          {:broadcast, String.t(), %{data: term(), exclude_socket: term()}}
          | {:command, String.t(), map()}

  @doc """
  Decode a raw broadcast payload into a list of items.

  Invalid elements of a batch are skipped. A payload that is valid JSON
  but not an envelope, a command or an array returns
  `{:error, :not_an_envelope}`.
  """
  @spec decode(binary()) :: {:ok, [item()]} | {:error, :invalid_json | :not_an_envelope}
  def decode(payload) when is_binary(payload) do
    case Jason.decode(payload) do
      {:ok, list} when is_list(list) ->
        {:ok, Enum.flat_map(list, &List.wrap(item(&1)))}

      {:ok, decoded} ->
        case item(decoded) do
          nil -> {:error, :not_an_envelope}
          item -> {:ok, [item]}
        end

      {:error, _} ->
        {:error, :invalid_json}
    end
  end

  @doc """
  Dispatch decoded items through `Embassy.Rails.Broadcast.dispatch/4`.

  Commands are logged at debug level and otherwise ignored: remote
  disconnect is not implemented.
  """
  @spec dispatch_items(atom(), [item()], Broadcast.scope()) :: :ok
  def dispatch_items(instance, items, scope) do
    Enum.each(items, fn
      {:broadcast, stream, payload} ->
        Broadcast.dispatch(instance, stream, payload, scope)

      {:command, name, _payload} ->
        Logger.debug("Embassy.Rails: ignoring AnyCable command #{name}")
    end)
  end

  defp item(%{"stream" => stream} = envelope) when is_binary(stream) do
    {:broadcast, stream, %{data: envelope["data"], exclude_socket: exclude_socket(envelope)}}
  end

  defp item(%{"command" => name} = command) when is_binary(name) do
    payload =
      case command["payload"] do
        %{} = map -> map
        _ -> %{}
      end

    {:command, name, payload}
  end

  defp item(_), do: nil

  defp exclude_socket(%{"meta" => %{"exclude_socket" => socket}}), do: socket
  defp exclude_socket(_), do: nil
end
