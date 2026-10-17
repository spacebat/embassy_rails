defmodule Embassy.Rails.Connection do
  @moduledoc """
  `WebSock` handler that terminates an ActionCable WebSocket and
  proxies the protocol to Rails via AnyCable HTTP RPC.

  Instantiated by `Embassy.Rails.Upgrade` via
  `WebSockAdapter.upgrade/4`. The state map carries the embassy_rails
  instance name, the AnyCable connection identifiers + state, and the
  active stream subscriptions.

  ## Presence

  AnyCable presence (see `Embassy.Rails.Presence`) is driven two ways:

    * from Rails, when a command reply carries `presence` (a channel calling
      `join_presence` / `leave_presence`). The presence stream is the `$p`
      key of that reply's `istate`, remembered per channel identifier.
    * from the client over `actioncable-v1-ext-json`: the `join`, `leave`
      and `presence` commands, and a `presence` object on `subscribe`.
      These are handled here and never sent to Rails.

  Join and leave events reach subscribers as
  `{"type":"presence","identifier":...,"message":{"type":"join"|"leave",...}}`.
  Unlike anycable-go, a `presence` command on a channel with no presence
  stream replies with `"message":{"type":"error"}` rather than nothing, so
  the JS client's `info()` promise rejects instead of hanging.

  See the v0.1 spec at
  `docs/superpowers/specs/2026-05-19-embassy-rails-v0.1-design.md` for
  the design rationale and error-handling matrix.
  """

  @behaviour WebSock

  alias Embassy.Rails.{Broadcast, Config, Presence, RPC}

  require Logger

  @impl WebSock
  def init(state) do
    case RPC.connect(state.instance, build_env(state)) do
      {:ok, %{"status" => 1} = resp} ->
        state = merge_env(%{state | identifiers: resp["identifiers"]}, resp["env"])

        :telemetry.execute(
          [:embassy_rails, :ws, :connection, :open],
          %{monotonic_time: System.monotonic_time()},
          %{instance: state.instance, channel_identifier: state.identifiers}
        )

        Registry.register(Embassy.Rails.connections_name(state.instance), :conn, nil)
        schedule_ping(state)
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

      {:ok, %{"command" => command, "identifier" => identifier} = msg}
      when command in ["join", "leave", "presence"] ->
        handle_presence_command(command, identifier, msg, state)

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
    schedule_ping(state)
    {:push, frame, state}
  end

  def handle_info({:cable_broadcast, stream, payload}, state) do
    {:message_queue_len, queued} = Process.info(self(), :message_queue_len)

    if queued > Config.snapshot(state.instance).max_mailbox do
      Logger.warning("Embassy.Rails.Connection: slow client, #{queued} queued; disconnecting")

      frame =
        {:text, Jason.encode!(%{type: "disconnect", reason: "slow_client", reconnect: true})}

      {:stop, :normal, {1008, "slow_client"}, [frame], state}
    else
      push_broadcast(stream, payload, state)
    end
  end

  def handle_info(:embassy_rails_drain, state) do
    frame =
      {:text, Jason.encode!(%{type: "disconnect", reason: "server_restart", reconnect: true})}

    {:stop, :normal, {1001, "server_restart"}, [frame], state}
  end

  def handle_info({:cable_presence, stream, event}, state) do
    case state.streams[stream] do
      nil -> {:ok, state}
      identifier -> {:push, presence_frame(identifier, event), state}
    end
  end

  def handle_info(msg, state) do
    Logger.debug("Embassy.Rails.Connection: ignoring info: #{inspect(msg)}")
    {:ok, state}
  end

  @impl WebSock
  def terminate(reason, %{identifiers: nil} = state) do
    :telemetry.execute(
      [:embassy_rails, :ws, :connection, :close],
      %{monotonic_time: System.monotonic_time()},
      %{instance: state.instance, channel_identifier: nil, reason: reason}
    )

    :ok
  end

  def terminate(reason, state) do
    RPC.disconnect(state.instance, %{
      identifiers: state.identifiers,
      subscriptions: state.streams |> Map.values() |> Enum.uniq(),
      env: build_env(state)
    })

    :telemetry.execute(
      [:embassy_rails, :ws, :connection, :close],
      %{monotonic_time: System.monotonic_time()},
      %{instance: state.instance, channel_identifier: state.identifiers, reason: reason}
    )

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

  defp schedule_ping(state) do
    case Config.snapshot(state.instance).ping_interval do
      :infinity -> :ok
      interval -> Process.send_after(self(), :ping, interval)
    end
  end

  defp push_broadcast(stream, payload, state) do
    with identifier when not is_nil(identifier) <- state.streams[stream],
         false <- payload[:exclude_socket] == state.socket_id do
      frame =
        Jason.encode!(%{identifier: identifier, message: decode_data(payload[:data])})

      {:push, {:text, frame}, state}
    else
      _ -> {:ok, state}
    end
  end

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
        resp
        |> apply_command_response(identifier, state)
        |> after_command(command, status, msg)

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
      |> merge_env(resp["env"])
      |> add_streams(resp["streams"] || [], identifier)
      |> drop_streams(resp["stopped_streams"] || [])
      |> maybe_stop_all_streams(resp["stop_streams"], identifier)
      |> put_presence_stream(identifier, get_in(resp, ["env", "istate", "$p"]))
      |> apply_rpc_presence(identifier, resp["presence"])

    frames = Enum.map(resp["transmissions"] || [], &{:text, &1})

    case {resp["disconnect"], frames} do
      {true, _} -> {:stop, :normal, {1000, "server_disconnect"}, frames, state}
      {_, []} -> {:ok, state}
      _ -> {:push, frames, state}
    end
  end

  # --- presence ---

  # Presence on subscribe (ext-json clients rejoining) and leave on
  # unsubscribe apply only once the RPC confirmed the command.
  defp after_command(result, "subscribe", 1, %{"identifier" => identifier} = msg) do
    case msg["presence"] do
      %{"id" => _} = presence ->
        map_state(result, &presence_join(&1, identifier, presence["id"], presence["info"]))

      _ ->
        result
    end
  end

  defp after_command(result, "unsubscribe", 1, %{"identifier" => identifier}) do
    map_state(result, fn state ->
      case Map.pop(state.presence_streams, identifier) do
        {nil, _} ->
          state

        {stream, presence_streams} ->
          :ok = Presence.leave(state.instance, stream, state.socket_id)
          %{state | presence_streams: presence_streams}
      end
    end)
  end

  defp after_command(result, _command, _status, _msg), do: result

  defp map_state({:ok, state}, fun), do: {:ok, fun.(state)}
  defp map_state({:push, frames, state}, fun), do: {:push, frames, fun.(state)}

  defp map_state({:stop, reason, close, frames, state}, fun),
    do: {:stop, reason, close, frames, fun.(state)}

  defp put_presence_stream(state, identifier, stream) when is_binary(stream) and stream != "" do
    %{state | presence_streams: Map.put(state.presence_streams, identifier, stream)}
  end

  defp put_presence_stream(state, _identifier, _stream), do: state

  defp apply_rpc_presence(state, identifier, %{"type" => "join", "id" => id} = presence) do
    presence_join(state, identifier, id, decode_info(presence["info"]))
  end

  defp apply_rpc_presence(state, identifier, %{"type" => "leave"}) do
    presence_leave(state, identifier)
  end

  defp apply_rpc_presence(state, _identifier, _presence), do: state

  defp handle_presence_command(
         "join",
         identifier,
         %{"presence" => %{"id" => id} = presence},
         state
       ) do
    {:ok, presence_join(state, identifier, id, presence["info"])}
  end

  defp handle_presence_command("join", _identifier, _msg, state) do
    Logger.warning("Embassy.Rails.Connection: presence join without presence data")
    {:ok, state}
  end

  defp handle_presence_command("leave", identifier, _msg, state) do
    {:ok, presence_leave(state, identifier)}
  end

  defp handle_presence_command("presence", identifier, msg, state) do
    return_records? =
      case msg["data"] do
        %{"return_records" => false} -> false
        _ -> true
      end

    # anycable-go stays silent when the identifier has no presence stream;
    # replying with an error lets the JS client's info() promise reject
    # instead of hanging.
    message =
      with stream when is_binary(stream) <- state.presence_streams[identifier],
           {:ok, info} <- Presence.info(state.instance, stream, return_records?) do
        info
      else
        _ -> %{"type" => "error"}
      end

    {:push, presence_frame(identifier, message), state}
  end

  defp presence_join(state, identifier, id, info) do
    with stream when is_binary(stream) <- state.presence_streams[identifier],
         :ok <- Presence.join(state.instance, stream, state.socket_id, to_string(id), info) do
      state
    else
      nil ->
        Logger.warning("Embassy.Rails.Connection: presence join without a presence stream")
        state

      {:error, reason} ->
        Logger.warning("Embassy.Rails.Connection: presence join failed: #{inspect(reason)}")
        state
    end
  end

  defp presence_leave(state, identifier) do
    case state.presence_streams[identifier] do
      nil ->
        Logger.warning("Embassy.Rails.Connection: presence leave without a presence stream")
        state

      stream ->
        :ok = Presence.leave(state.instance, stream, state.socket_id)
        state
    end
  end

  defp presence_frame(identifier, message) do
    {:text, Jason.encode!(%{type: "presence", identifier: identifier, message: message})}
  end

  defp decode_info(nil), do: nil
  defp decode_info(""), do: nil
  defp decode_info(info), do: decode_data(info)

  # AnyCable's EnvResponse carries only the keys that changed; merge them
  # (as anycable-go does) rather than replace the whole state.
  defp merge_env(state, nil), do: state

  defp merge_env(state, env) do
    %{
      state
      | cstate: Map.merge(state.cstate, env["cstate"] || %{}),
        istate: Map.merge(state.istate, env["istate"] || %{})
    }
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
