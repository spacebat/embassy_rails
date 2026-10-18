defmodule Embassy.Rails.RPCTest do
  use ExUnit.Case, async: true

  alias Embassy.Rails.RPC
  alias Embassy.Rails.Test.TelemetryCollector

  setup do
    start_supervised!({Finch, name: Embassy.Rails.RPCTest.Finch})
    start_supervised!({Phoenix.PubSub, name: Embassy.Rails.RPCTest.PubSub})

    start_supervised!(
      {Embassy, name: Embassy.Rails.RPCTest.Embassy, finch: Embassy.Rails.RPCTest.Finch}
    )

    bypass = Bypass.open()

    start_supervised!(
      {Embassy.Rails,
       name: Embassy.Rails.RPCTest.Instance,
       embassy: Embassy.Rails.RPCTest.Embassy,
       pubsub: Embassy.Rails.RPCTest.PubSub,
       rpc: [transport: {:http, "http://localhost:#{bypass.port}"}, secret: "rpc-secret"],
       broadcast: [source: :http, key: "bcast"]}
    )

    {:ok, bypass: bypass, instance: Embassy.Rails.RPCTest.Instance}
  end

  test "connect/2 posts the envelope and decodes a SUCCESS response", %{
    bypass: bypass,
    instance: instance
  } do
    Bypass.expect_once(bypass, "POST", "/_anycable/connect", fn conn ->
      assert ["Bearer rpc-secret"] = Plug.Conn.get_req_header(conn, "authorization")
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      assert Jason.decode!(body) == %{
               "env" => %{"url" => "wss://t/", "headers" => %{}, "cstate" => %{}, "istate" => %{}}
             }

      conn
      |> Plug.Conn.put_resp_header("content-type", "application/json")
      |> Plug.Conn.resp(200, ~s({"status":1,"identifiers":"abc","transmissions":[]}))
    end)

    env = %{url: "wss://t/", headers: %{}, cstate: %{}, istate: %{}}
    assert {:ok, resp} = RPC.connect(instance, env)
    assert resp["status"] == 1
    assert resp["identifiers"] == "abc"
  end

  test "command/2 posts the envelope and decodes a response", %{
    bypass: bypass,
    instance: instance
  } do
    Bypass.expect_once(bypass, "POST", "/_anycable/command", fn conn ->
      Plug.Conn.resp(conn, 200, ~s({"status":1,"transmissions":["msg"]}))
    end)

    payload = %{command: "subscribe", identifier: "{\"channel\":\"X\"}"}
    assert {:ok, %{"status" => 1, "transmissions" => ["msg"]}} = RPC.command(instance, payload)
  end

  test "disconnect/2 fires the call and returns :ok regardless", %{
    bypass: bypass,
    instance: instance
  } do
    test_pid = self()

    Bypass.expect_once(bypass, "POST", "/_anycable/disconnect", fn conn ->
      send(test_pid, {:disconnect_posted, self()})

      receive do
        :release -> Plug.Conn.resp(conn, 200, "{}")
      end
    end)

    assert :ok == RPC.disconnect(instance, %{identifiers: "abc", subscriptions: []})
    assert_receive {:disconnect_posted, handler}, 1_000

    tasks = Embassy.Rails.tasks_name(instance)
    assert [task] = Task.Supervisor.children(tasks)
    ref = Process.monitor(task)

    send(handler, :release)
    assert_receive {:DOWN, ^ref, :process, ^task, :normal}, 1_000
  end

  @tag :capture_log
  test "disconnect/2 drops a disconnect over the rpc :max_disconnects cap", %{bypass: bypass} do
    instance = __MODULE__.Capped
    test_pid = self()

    start_supervised!(
      {Embassy.Rails,
       name: instance,
       embassy: Embassy.Rails.RPCTest.Embassy,
       pubsub: Embassy.Rails.RPCTest.PubSub,
       rpc: [
         transport: {:http, "http://localhost:#{bypass.port}"},
         secret: "rpc-secret",
         max_disconnects: 1
       ],
       broadcast: [source: :http, key: "bcast"]},
      id: instance
    )

    Bypass.expect_once(bypass, "POST", "/_anycable/disconnect", fn conn ->
      send(test_pid, {:disconnect_posted, self()})

      receive do
        :release -> Plug.Conn.resp(conn, 200, "{}")
      end
    end)

    assert :ok == RPC.disconnect(instance, %{identifiers: "a", subscriptions: []})
    assert_receive {:disconnect_posted, handler}, 1_000

    assert :ok == RPC.disconnect(instance, %{identifiers: "b", subscriptions: []})
    assert [task] = Task.Supervisor.children(Embassy.Rails.tasks_name(instance))
    ref = Process.monitor(task)

    send(handler, :release)
    assert_receive {:DOWN, ^ref, :process, ^task, :normal}, 1_000
  end

  test "disconnect/2 is a no-op for an instance that is not running" do
    assert :ok == RPC.disconnect(__MODULE__.NotRunning, %{identifiers: "a", subscriptions: []})
  end

  test "non-200 response returns {:error, {:http, status, body}}", %{
    bypass: bypass,
    instance: instance
  } do
    Bypass.expect_once(bypass, "POST", "/_anycable/connect", fn conn ->
      Plug.Conn.resp(conn, 500, "boom")
    end)

    assert {:error, {:http, 500, "boom"}} =
             RPC.connect(instance, %{url: "wss://t/", headers: %{}, cstate: %{}, istate: %{}})
  end

  test "transport error returns {:error, reason}", %{bypass: bypass, instance: instance} do
    Bypass.down(bypass)

    assert {:error, _reason} =
             RPC.connect(instance, %{url: "wss://t/", headers: %{}, cstate: %{}, istate: %{}})
  end

  test "emits :start and :stop telemetry on success", %{bypass: bypass, instance: instance} do
    {:ok, collector} =
      TelemetryCollector.attach([
        [:embassy_rails, :rpc, :connect, :start],
        [:embassy_rails, :rpc, :connect, :stop]
      ])

    Bypass.expect_once(bypass, "POST", "/_anycable/connect", fn conn ->
      Plug.Conn.resp(conn, 200, ~s({"status":1}))
    end)

    RPC.connect(instance, %{url: "wss://t/", headers: %{}, cstate: %{}, istate: %{}})

    events = TelemetryCollector.events(collector)

    assert Enum.any?(events, fn {name, _, _} ->
             name == [:embassy_rails, :rpc, :connect, :start]
           end)

    assert Enum.any?(events, fn {name, _, _} ->
             name == [:embassy_rails, :rpc, :connect, :stop]
           end)
  end

  test "emits :exception telemetry on transport failure", %{bypass: bypass, instance: instance} do
    {:ok, collector} =
      TelemetryCollector.attach([[:embassy_rails, :rpc, :exception]])

    Bypass.down(bypass)

    RPC.connect(instance, %{url: "wss://t/", headers: %{}, cstate: %{}, istate: %{}})

    # Telemetry handlers attach globally per event name, so a concurrent
    # async test emitting [:embassy_rails, :rpc, :exception] for a different
    # instance would also land in this collector. Scope to our instance.
    events =
      TelemetryCollector.events(collector)
      |> Enum.filter(fn {_name, _measurements, meta} -> meta.instance == instance end)

    assert [{[:embassy_rails, :rpc, :exception], _measurements, meta}] = events
    assert meta.kind == :connect_error
  end

  test "rpc :timeout bounds the HTTP call" do
    # A listener that never answers: the kernel completes the TCP handshake
    # from the backlog, so the request is sent and the reply never comes.
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(listener)
    on_exit(fn -> :gen_tcp.close(listener) end)

    start_supervised!(
      {Embassy.Rails,
       name: __MODULE__.Timeout,
       embassy: Embassy.Rails.RPCTest.Embassy,
       pubsub: Embassy.Rails.RPCTest.PubSub,
       rpc: [transport: {:http, "http://localhost:#{port}"}, secret: "rpc-secret", timeout: 50],
       broadcast: [source: :http, key: "bcast"]},
      id: :timeout_instance
    )

    {elapsed_us, result} =
      :timer.tc(fn -> RPC.command(__MODULE__.Timeout, %{command: "message"}) end)

    assert {:error, %{reason: :timeout}} = result
    assert elapsed_us < 1_000_000
  end
end
