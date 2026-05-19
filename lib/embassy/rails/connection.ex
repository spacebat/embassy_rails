defmodule Embassy.Rails.Connection do
  @moduledoc """
  `WebSock` handler that terminates an ActionCable WebSocket and
  proxies the protocol to Rails via AnyCable HTTP RPC.

  Instantiated by `Embassy.Rails.Upgrade` via
  `WebSockAdapter.upgrade/4`. The state map carries the embassy_rails
  instance name, the AnyCable connection identifiers + state, and the
  active stream subscriptions.

  See the v0.1 spec at
  `docs/superpowers/specs/2026-05-19-embassy-rails-v0.1-design.md` for
  the design rationale and error-handling matrix.
  """

  @behaviour WebSock

  alias Embassy.Rails.{Broadcast, Config, RPC}

  require Logger

  @ping_interval 3_000

  @impl WebSock
  def init(state) do
    case RPC.connect(state.instance, build_env(state)) do
      {:ok, %{"status" => 1} = resp} ->
        state =
          %{
            state
            | identifiers: resp["identifiers"],
              cstate: get_in(resp, ["env", "cstate"]) || %{},
              istate: get_in(resp, ["env", "istate"]) || %{}
          }

        schedule_ping()
        {:push, connect_frames(resp["transmissions"] || []), state}

      {:ok, %{"status" => 2}} ->
        disconnect_frame =
          {:text, Jason.encode!(%{type: "disconnect", reason: "unauthorized", reconnect: false})}

        {:stop, :normal, {1011, "unauthorized"}, [disconnect_frame], state}

      {:error, reason} ->
        Logger.error("Embassy.Rails.Connection: connect RPC failed: #{inspect(reason)}")
        {:stop, :normal, {1011, "rpc_error"}, state}
    end
  end

  @impl WebSock
  def handle_in({_payload, [opcode: opcode]}, state) when opcode != :text do
    Logger.debug("Embassy.Rails.Connection: dropping #{opcode} frame")
    {:ok, state}
  end

  def handle_in({payload, [opcode: :text]}, state) do
    case Jason.decode(payload) do
      {:ok, %{"command" => command} = msg}
      when command in ["subscribe", "unsubscribe", "message"] ->
        handle_command(command, msg, state)

      {:ok, other} ->
        Logger.debug("Embassy.Rails.Connection: ignoring unknown frame: #{inspect(other)}")
        {:ok, state}

      {:error, _} ->
        Logger.warning("Embassy.Rails.Connection: malformed JSON frame")
        {:ok, state}
    end
  end

  @impl WebSock
  def handle_info(:ping, state) do
    frame = {:text, Jason.encode!(%{type: "ping", message: System.system_time(:second)})}
    schedule_ping()
    {:push, frame, state}
  end

  def handle_info({:cable_broadcast, stream, payload}, state) do
    with identifier when not is_nil(identifier) <- state.streams[stream],
         false <- payload[:exclude_socket] == state.socket_id do
      frame =
        Jason.encode!(%{identifier: identifier, message: decode_data(payload[:data])})

      {:push, {:text, frame}, state}
    else
      _ -> {:ok, state}
    end
  end

  def handle_info(msg, state) do
    Logger.debug("Embassy.Rails.Connection: ignoring info: #{inspect(msg)}")
    {:ok, state}
  end

  @impl WebSock
  def terminate(_reason, %{identifiers: nil}), do: :ok

  def terminate(_reason, state) do
    RPC.disconnect(state.instance, %{
      identifiers: state.identifiers,
      subscriptions: state.streams |> Map.values() |> Enum.uniq(),
      env: build_env(state)
    })

    :ok
  end

  # --- private helpers ---

  defp build_env(state) do
    %{url: state.url, headers: state.headers, cstate: state.cstate, istate: state.istate}
  end

  defp connect_frames([first | _] = transmissions) do
    if welcome?(first),
      do: Enum.map(transmissions, &{:text, &1}),
      else: Enum.map(transmissions, &{:text, &1}) ++ [welcome_frame()]
  end

  defp connect_frames([]), do: [welcome_frame()]

  defp welcome_frame, do: {:text, Jason.encode!(%{type: "welcome"})}

  defp welcome?(transmission) do
    match?({:ok, %{"type" => "welcome"}}, Jason.decode(transmission))
  end

  defp schedule_ping, do: Process.send_after(self(), :ping, @ping_interval)

  defp handle_command(command, %{"identifier" => identifier} = msg, state) do
    payload = %{
      command: command,
      identifier: identifier,
      data: msg["data"] || "",
      connection_identifiers: state.identifiers,
      env: build_env(state)
    }

    case RPC.command(state.instance, payload) do
      {:ok, %{"status" => status} = resp} when status in [1, 2] ->
        apply_command_response(resp, identifier, state)

      {:ok, resp} ->
        Logger.warning("Embassy.Rails.Connection: error command response=#{inspect(resp)}")
        {:ok, state}

      {:error, reason} ->
        Logger.error("Embassy.Rails.Connection: command RPC failed: #{inspect(reason)}")
        {:ok, state}
    end
  end

  defp handle_command(_command, _msg, state), do: {:ok, state}

  defp apply_command_response(resp, identifier, state) do
    state =
      state
      |> Map.put(:cstate, get_in(resp, ["env", "cstate"]) || state.cstate)
      |> Map.put(:istate, get_in(resp, ["env", "istate"]) || state.istate)
      |> add_streams(resp["streams"] || [], identifier)
      |> drop_streams(resp["stopped_streams"] || [])
      |> maybe_stop_all_streams(resp["stop_streams"], identifier)

    frames = Enum.map(resp["transmissions"] || [], &{:text, &1})

    case {resp["disconnect"], frames} do
      {true, _} -> {:stop, :normal, {1000, "server_disconnect"}, frames, state}
      {_, []} -> {:ok, state}
      _ -> {:push, frames, state}
    end
  end

  defp add_streams(state, streams, identifier) do
    snap = Config.snapshot(state.instance)

    Enum.reduce(streams, state, fn stream, acc ->
      if Map.has_key?(acc.streams, stream) do
        acc
      else
        Phoenix.PubSub.subscribe(snap.pubsub, Broadcast.topic(stream))
        %{acc | streams: Map.put(acc.streams, stream, identifier)}
      end
    end)
  end

  defp drop_streams(state, streams) do
    snap = Config.snapshot(state.instance)

    Enum.reduce(streams, state, fn stream, acc ->
      Phoenix.PubSub.unsubscribe(snap.pubsub, Broadcast.topic(stream))
      %{acc | streams: Map.delete(acc.streams, stream)}
    end)
  end

  defp maybe_stop_all_streams(state, true, identifier) do
    snap = Config.snapshot(state.instance)

    streams =
      Map.reject(state.streams, fn {stream, id} ->
        if id == identifier do
          Phoenix.PubSub.unsubscribe(snap.pubsub, Broadcast.topic(stream))
          true
        else
          false
        end
      end)

    %{state | streams: streams}
  end

  defp maybe_stop_all_streams(state, _, _), do: state

  # AnyCable broadcast payloads include `data` as a pre-serialized JSON
  # string. ActionCable's client wants the parsed object under `message`,
  # so we re-parse here. Non-JSON data passes through verbatim.
  defp decode_data(nil), do: nil

  defp decode_data(data) when is_binary(data) do
    case Jason.decode(data) do
      {:ok, parsed} -> parsed
      {:error, _} -> data
    end
  end

  defp decode_data(data), do: data
end
