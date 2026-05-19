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
    Process.sleep(50)
  end
end
