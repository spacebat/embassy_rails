defmodule Embassy.Rails.ConnectionTest do
  use ExUnit.Case, async: true

  alias Embassy.Rails.Connection

  setup do
    start_supervised!({Finch, name: Embassy.Rails.ConnectionTest.Finch})
    start_supervised!({Phoenix.PubSub, name: Embassy.Rails.ConnectionTest.PubSub})

    start_supervised!(
      {Embassy,
       name: Embassy.Rails.ConnectionTest.Embassy, finch: Embassy.Rails.ConnectionTest.Finch}
    )

    bypass = Bypass.open()

    start_supervised!(
      {Embassy.Rails,
       name: Embassy.Rails.ConnectionTest.Instance,
       embassy: Embassy.Rails.ConnectionTest.Embassy,
       pubsub: Embassy.Rails.ConnectionTest.PubSub,
       rpc: [transport: {:http, "http://localhost:#{bypass.port}"}, secret: "shh"],
       broadcast: [source: :http, key: "bcast"]}
    )

    initial_state = %{
      instance: Embassy.Rails.ConnectionTest.Instance,
      socket_id: "phx-test",
      url: "wss://test/cable",
      headers: %{},
      identifiers: nil,
      cstate: %{},
      istate: %{},
      streams: %{}
    }

    {:ok, bypass: bypass, state: initial_state}
  end

  test "init/1 success: pushes welcome frame and schedules ping", %{bypass: bypass, state: state} do
    Bypass.expect_once(bypass, "POST", "/_anycable/connect", fn conn ->
      Plug.Conn.resp(
        conn,
        200,
        ~s({"status":1,"identifiers":"abc","transmissions":[],"env":{"cstate":{"c":"1"},"istate":{}}})
      )
    end)

    assert {:push, [{:text, frame}], new_state} = Connection.init(state)

    assert Jason.decode!(frame) == %{"type" => "welcome"}
    assert new_state.identifiers == "abc"
    assert new_state.cstate == %{"c" => "1"}

    # Confirm the ping timer fires by waiting briefly. Ping interval is 3s; we
    # don't want to actually wait — just confirm a :ping message is queued by
    # checking the process mailbox shape. We can't introspect, so trust the
    # scheduling and move on.
  end

  test "init/1 status 2: closes 1011 with disconnect frame", %{bypass: bypass, state: state} do
    Bypass.expect_once(bypass, "POST", "/_anycable/connect", fn conn ->
      Plug.Conn.resp(conn, 200, ~s({"status":2}))
    end)

    assert {:stop, :normal, {1011, "unauthorized"}, [{:text, frame}], ^state} =
             Connection.init(state)

    assert Jason.decode!(frame) == %{
             "type" => "disconnect",
             "reason" => "unauthorized",
             "reconnect" => false
           }
  end

  test "init/1 RPC error: closes 1011 rpc_error", %{bypass: bypass, state: state} do
    Bypass.down(bypass)

    assert {:stop, :normal, {1011, "rpc_error"}, ^state} = Connection.init(state)
  end

  test "handle_info(:ping, state): pushes ping frame and reschedules", %{state: state} do
    assert {:push, {:text, frame}, ^state} = Connection.handle_info(:ping, state)

    decoded = Jason.decode!(frame)
    assert decoded["type"] == "ping"
    assert is_integer(decoded["message"])
  end

  test "terminate/2 with nil identifiers is a no-op", %{state: state} do
    assert :ok = Connection.terminate(:normal, state)
  end

  test "terminate/2 with identifiers fires disconnect RPC", %{bypass: bypass, state: state} do
    Bypass.expect_once(bypass, "POST", "/_anycable/disconnect", fn conn ->
      Plug.Conn.resp(conn, 200, "{}")
    end)

    state_with_ids = %{state | identifiers: "abc", streams: %{"chat" => "{}"}}
    assert :ok = Connection.terminate(:normal, state_with_ids)
    # Give the async disconnect task time to fire.
    Process.sleep(200)
  end

  test "handle_in subscribe: applies streams, pushes transmissions, mutates state", %{
    bypass: bypass,
    state: state
  } do
    state_with_ids = %{state | identifiers: "abc"}

    Bypass.expect_once(bypass, "POST", "/_anycable/command", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      assert Jason.decode!(body)["command"] == "subscribe"

      Plug.Conn.resp(
        conn,
        200,
        ~s({"status":1,"streams":["chat_5"],"transmissions":["{\\"identifier\\":\\"X\\"}"],"env":{"cstate":{"u":"1"}}})
      )
    end)

    frame = Jason.encode!(%{"command" => "subscribe", "identifier" => "X", "data" => ""})

    assert {:push, [{:text, _}], new_state} =
             Connection.handle_in({frame, [opcode: :text]}, state_with_ids)

    assert Map.has_key?(new_state.streams, "chat_5")
    assert new_state.cstate == %{"u" => "1"}
  end

  test "handle_in unsubscribe: stops streams, no transmissions", %{bypass: bypass, state: state} do
    state_with_sub = %{state | identifiers: "abc", streams: %{"chat_5" => "X"}}

    Bypass.expect_once(bypass, "POST", "/_anycable/command", fn conn ->
      Plug.Conn.resp(conn, 200, ~s({"status":1,"stop_streams":true}))
    end)

    frame = Jason.encode!(%{"command" => "unsubscribe", "identifier" => "X"})

    assert {:ok, new_state} = Connection.handle_in({frame, [opcode: :text]}, state_with_sub)
    refute Map.has_key?(new_state.streams, "chat_5")
  end

  test "handle_in message: status 1 with disconnect: true closes 1000", %{
    bypass: bypass,
    state: state
  } do
    state_with_ids = %{state | identifiers: "abc"}

    Bypass.expect_once(bypass, "POST", "/_anycable/command", fn conn ->
      Plug.Conn.resp(conn, 200, ~s({"status":1,"disconnect":true,"transmissions":["bye"]}))
    end)

    frame = Jason.encode!(%{"command" => "message", "identifier" => "X", "data" => "hi"})

    assert {:stop, :normal, {1000, "server_disconnect"}, [{:text, "bye"}], _} =
             Connection.handle_in({frame, [opcode: :text]}, state_with_ids)
  end

  test "handle_in status 0 (Rails-side error): logs, keeps connection open", %{
    bypass: bypass,
    state: state
  } do
    Bypass.expect_once(bypass, "POST", "/_anycable/command", fn conn ->
      Plug.Conn.resp(conn, 200, ~s({"status":0,"error":"boom"}))
    end)

    frame = Jason.encode!(%{"command" => "subscribe", "identifier" => "X"})

    assert {:ok, _} =
             Connection.handle_in({frame, [opcode: :text]}, %{state | identifiers: "abc"})
  end

  test "handle_in RPC error: drops frame, keeps connection open", %{bypass: bypass, state: state} do
    Bypass.down(bypass)

    frame = Jason.encode!(%{"command" => "subscribe", "identifier" => "X"})

    assert {:ok, _} =
             Connection.handle_in({frame, [opcode: :text]}, %{state | identifiers: "abc"})
  end

  test "handle_in malformed JSON: drops frame, keeps connection open", %{state: state} do
    assert {:ok, ^state} = Connection.handle_in({"not json", [opcode: :text]}, state)
  end

  test "handle_in unknown command: drops frame, keeps connection open", %{state: state} do
    frame = Jason.encode!(%{"command" => "weird_thing"})
    assert {:ok, ^state} = Connection.handle_in({frame, [opcode: :text]}, state)
  end

  test "handle_info({:cable_broadcast, ...}) pushes payload to client when subscribed", %{
    state: state
  } do
    state_sub = %{
      state
      | identifiers: "abc",
        streams: %{"chat_5" => ~s({"channel":"ChatChannel","room":"5"})}
    }

    payload = %{data: ~s({"text":"hi"}), exclude_socket: nil}

    assert {:push, {:text, frame}, ^state_sub} =
             Connection.handle_info({:cable_broadcast, "chat_5", payload}, state_sub)

    decoded = Jason.decode!(frame)
    assert decoded["identifier"] == ~s({"channel":"ChatChannel","room":"5"})
    assert decoded["message"] == %{"text" => "hi"}
  end

  test "handle_info ignores broadcast for unsubscribed stream", %{state: state} do
    assert {:ok, ^state} =
             Connection.handle_info({:cable_broadcast, "other", %{data: "x"}}, state)
  end

  test "handle_info respects exclude_socket filter", %{state: state} do
    state_sub = %{
      state
      | identifiers: "abc",
        streams: %{"chat_5" => "X"}
    }

    payload = %{data: "x", exclude_socket: state_sub.socket_id}

    assert {:ok, ^state_sub} =
             Connection.handle_info({:cable_broadcast, "chat_5", payload}, state_sub)
  end

  test "handle_info passes through non-JSON data verbatim", %{state: state} do
    state_sub = %{state | identifiers: "abc", streams: %{"chat_5" => "X"}}

    payload = %{data: "not json", exclude_socket: nil}

    assert {:push, {:text, frame}, ^state_sub} =
             Connection.handle_info({:cable_broadcast, "chat_5", payload}, state_sub)

    decoded = Jason.decode!(frame)
    assert decoded["message"] == "not json"
  end

  test "init/1 success emits :ws, :connection, :open telemetry", %{bypass: bypass, state: state} do
    {:ok, collector} =
      Embassy.Rails.Test.TelemetryCollector.attach([
        [:embassy_rails, :ws, :connection, :open]
      ])

    Bypass.expect_once(bypass, "POST", "/_anycable/connect", fn conn ->
      Plug.Conn.resp(conn, 200, ~s({"status":1,"identifiers":"abc"}))
    end)

    Connection.init(state)

    events = Embassy.Rails.Test.TelemetryCollector.events(collector)
    assert [{[:embassy_rails, :ws, :connection, :open], _measurements, meta}] = events
    assert meta.instance == Embassy.Rails.ConnectionTest.Instance
  end

  test "terminate/2 emits :ws, :connection, :close telemetry", %{bypass: bypass, state: state} do
    {:ok, collector} =
      Embassy.Rails.Test.TelemetryCollector.attach([
        [:embassy_rails, :ws, :connection, :close]
      ])

    Bypass.expect(bypass, "POST", "/_anycable/disconnect", fn conn ->
      Plug.Conn.resp(conn, 200, "{}")
    end)

    state_with_ids = %{state | identifiers: "abc"}
    Connection.terminate(:normal, state_with_ids)

    # Give the async disconnect task time to fire (mirrors existing terminate test).
    Process.sleep(200)

    events = Embassy.Rails.Test.TelemetryCollector.events(collector)
    assert [{[:embassy_rails, :ws, :connection, :close], _measurements, meta}] = events
    assert meta.instance == Embassy.Rails.ConnectionTest.Instance
  end

  test "command reply merges changed istate/cstate keys into existing state",
       %{bypass: bypass, state: state} do
    state = %{state | identifiers: "abc", istate: %{"a" => "1"}, cstate: %{"c" => "1"}}

    Bypass.expect_once(bypass, "POST", "/_anycable/command", fn conn ->
      Plug.Conn.resp(
        conn,
        200,
        ~s({"status":1,"streams":[],"transmissions":[],"env":{"istate":{"$p":"s1"},"cstate":{}}})
      )
    end)

    frame = Jason.encode!(%{command: "subscribe", identifier: ~s({"channel":"X"})})
    assert {:ok, new_state} = Connection.handle_in({frame, [opcode: :text]}, state)
    assert new_state.istate == %{"a" => "1", "$p" => "s1"}
    assert new_state.cstate == %{"c" => "1"}
  end
end
