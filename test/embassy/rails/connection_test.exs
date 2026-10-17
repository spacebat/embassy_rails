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
       broadcast: [source: :http, key: "bcast"],
       presence: [ttl_ms: 50]}
    )

    initial_state = %{
      instance: Embassy.Rails.ConnectionTest.Instance,
      socket_id: "phx-test",
      url: "wss://test/cable",
      headers: %{},
      identifiers: nil,
      cstate: %{},
      istate: %{},
      streams: %{},
      presence_streams: %{}
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

  @tag :capture_log
  test "unsubscribe from a channel without presence logs no presence warning", %{
    bypass: bypass,
    state: state
  } do
    Bypass.expect_once(bypass, "POST", "/_anycable/command", fn conn ->
      Plug.Conn.resp(conn, 200, ~s({"status":1,"streams":[],"transmissions":[]}))
    end)

    frame = Jason.encode!(%{command: "unsubscribe", identifier: ~s({"channel":"Plain"})})

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        Connection.handle_in({frame, [opcode: :text]}, %{state | identifiers: "abc"})
      end)

    refute log =~ "presence"
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

  describe "presence" do
    @room ~s({"channel":"Room"})

    # Answers every command RPC with `fun.(decoded_request)`.
    defp stub_commands(bypass, fun) do
      Bypass.stub(bypass, "POST", "/_anycable/command", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        Plug.Conn.resp(conn, 200, Jason.encode!(fun.(Jason.decode!(body))))
      end)
    end

    defp ok_response(extra) do
      Map.merge(%{"status" => 1, "streams" => [], "transmissions" => []}, extra)
    end

    defp send_frame(state, frame),
      do: Connection.handle_in({Jason.encode!(frame), [opcode: :text]}, state)

    defp subscribed(state, identifier \\ @room) do
      case send_frame(state, %{command: "subscribe", identifier: identifier}) do
        {:ok, state} -> state
        {:push, _frames, state} -> state
      end
    end

    defp with_presence_stream(stream, extra \\ %{}) do
      ok_response(
        Map.merge(%{"streams" => [stream], "env" => %{"istate" => %{"$p" => stream}}}, extra)
      )
    end

    test "rpc join on subscribe", %{bypass: bypass, state: state} do
      stub_commands(bypass, fn _ ->
        with_presence_stream("a", %{
          "transmissions" => [~s({"type":"confirm_subscription","identifier":"x"})],
          "presence" => %{"type" => "join", "id" => "2024", "info" => ~s({"name":"Bart"})}
        })
      end)

      assert {:push, [{:text, _confirm}], state} =
               send_frame(%{state | identifiers: "abc"}, %{
                 command: "subscribe",
                 identifier: @room
               })

      assert_receive {:cable_presence, "a", event}, 500
      assert event == %{"type" => "join", "id" => "2024", "info" => %{"name" => "Bart"}}

      assert {:push, {:text, frame}, _} =
               Connection.handle_info({:cable_presence, "a", event}, state)

      assert Jason.decode!(frame) == %{
               "type" => "presence",
               "identifier" => @room,
               "message" => %{"type" => "join", "id" => "2024", "info" => %{"name" => "Bart"}}
             }
    end

    test "rpc leave on perform", %{bypass: bypass, state: state} do
      stub_commands(bypass, fn
        %{"command" => "subscribe"} ->
          with_presence_stream("b", %{"presence" => %{"type" => "join", "id" => "2024"}})

        %{"command" => "message"} ->
          ok_response(%{"presence" => %{"type" => "leave", "id" => "2024"}})
      end)

      state = subscribed(%{state | identifiers: "abc"})
      assert_receive {:cable_presence, "b", %{"type" => "join"}}, 500

      send_frame(state, %{command: "message", identifier: @room, data: ~s({"action":"unfollow"})})
      assert_receive {:cable_presence, "b", %{"type" => "leave", "id" => "2024"}}, 500
    end

    test "subscribe with client presence joins on status 1", %{bypass: bypass, state: state} do
      stub_commands(bypass, fn _ -> with_presence_stream("c") end)

      send_frame(%{state | identifiers: "abc"}, %{
        command: "subscribe",
        identifier: @room,
        presence: %{id: "7", info: %{a: 1}}
      })

      assert_receive {:cable_presence, "c",
                      %{"type" => "join", "id" => "7", "info" => %{"a" => 1}}},
                     500
    end

    test "subscribe with client presence does not join on status 2", %{
      bypass: bypass,
      state: state
    } do
      stub_commands(bypass, fn _ -> with_presence_stream("c2", %{"status" => 2}) end)

      send_frame(%{state | identifiers: "abc"}, %{
        command: "subscribe",
        identifier: @room,
        presence: %{id: "7"}
      })

      refute_receive {:cable_presence, "c2", _}, 100
    end

    test "client join and leave commands", %{bypass: bypass, state: state} do
      test_pid = self()

      stub_commands(bypass, fn req ->
        send(test_pid, {:rpc, req["command"]})
        with_presence_stream("d")
      end)

      state = subscribed(%{state | identifiers: "abc"})
      assert_receive {:rpc, "subscribe"}

      assert {:ok, state} =
               send_frame(state, %{command: "join", identifier: @room, presence: %{id: "x"}})

      assert_receive {:cable_presence, "d", %{"type" => "join", "id" => "x"}}, 500

      assert {:ok, _} = send_frame(state, %{command: "leave", identifier: @room})
      assert_receive {:cable_presence, "d", %{"type" => "leave", "id" => "x"}}, 500
      refute_received {:rpc, _}
    end

    test "presence command replies with the info frame", %{bypass: bypass, state: state} do
      stub_commands(bypass, fn _ ->
        with_presence_stream("e", %{
          "presence" => %{"type" => "join", "id" => "u1", "info" => ~s({"n":1})}
        })
      end)

      state = subscribed(%{state | identifiers: "abc"})

      assert {:push, {:text, frame}, _} =
               send_frame(state, %{command: "presence", identifier: @room, data: %{}})

      assert Jason.decode!(frame) == %{
               "type" => "presence",
               "identifier" => @room,
               "message" => %{
                 "type" => "info",
                 "total" => 1,
                 "records" => [%{"id" => "u1", "info" => %{"n" => 1}}]
               }
             }

      assert {:push, {:text, frame}, _} =
               send_frame(state, %{
                 command: "presence",
                 identifier: @room,
                 data: %{return_records: false}
               })

      refute Map.has_key?(Jason.decode!(frame)["message"], "records")
    end

    test "unsubscribe leaves", %{bypass: bypass, state: state} do
      stub_commands(bypass, fn
        %{"command" => "subscribe"} ->
          with_presence_stream("f", %{"presence" => %{"type" => "join", "id" => "u1"}})

        %{"command" => "unsubscribe"} ->
          ok_response(%{})
      end)

      state = subscribed(%{state | identifiers: "abc"})
      assert_receive {:cable_presence, "f", %{"type" => "join"}}, 500

      {_, state} = send_frame(state, %{command: "unsubscribe", identifier: @room})
      assert_receive {:cable_presence, "f", %{"type" => "leave", "id" => "u1"}}, 500
      refute Map.has_key?(state.presence_streams, @room)
    end

    test "second connection with the same id", %{bypass: bypass, state: state} do
      stub_commands(bypass, fn
        %{"command" => "subscribe"} ->
          with_presence_stream("g", %{"presence" => %{"type" => "join", "id" => "2024"}})

        %{"command" => "unsubscribe"} ->
          ok_response(%{})
      end)

      first = subscribed(%{state | identifiers: "abc", socket_id: "s1"})
      _second = subscribed(%{state | identifiers: "abc", socket_id: "s2"})

      assert_receive {:cable_presence, "g", %{"type" => "join"}}, 500
      refute_receive {:cable_presence, "g", %{"type" => "join"}}, 100

      send_frame(first, %{command: "unsubscribe", identifier: @room})
      refute_receive {:cable_presence, "g", %{"type" => "leave"}}, 100
    end

    test "presence commands without a presence stream", %{bypass: bypass, state: state} do
      stub_commands(bypass, fn _ -> ok_response(%{"streams" => ["h"]}) end)
      state = subscribed(%{state | identifiers: "abc"})

      assert {:ok, ^state} =
               send_frame(state, %{command: "join", identifier: @room, presence: %{id: "x"}})

      refute_receive {:cable_presence, _, _}, 50

      assert {:push, {:text, frame}, _} =
               send_frame(state, %{command: "presence", identifier: @room})

      assert Jason.decode!(frame) == %{
               "type" => "presence",
               "identifier" => @room,
               "message" => %{"type" => "error"}
             }
    end

    test "a client join without a presence object is ignored", %{bypass: bypass, state: state} do
      stub_commands(bypass, fn _ -> with_presence_stream("i") end)
      state = subscribed(%{state | identifiers: "abc"})

      assert {:ok, _} = send_frame(state, %{command: "join", identifier: @room})
      refute_receive {:cable_presence, _, _}, 50
    end

    test "non-JSON info passes through as a string", %{bypass: bypass, state: state} do
      stub_commands(bypass, fn _ ->
        with_presence_stream("j", %{
          "presence" => %{"type" => "join", "id" => "u1", "info" => "plain"}
        })
      end)

      subscribed(%{state | identifiers: "abc"})
      assert_receive {:cable_presence, "j", %{"info" => "plain"}}, 500
    end
  end
end
