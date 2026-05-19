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
    Bypass.expect_once(bypass, "POST", "/_anycable/disconnect", fn conn ->
      Plug.Conn.resp(conn, 200, "{}")
    end)

    assert :ok == RPC.disconnect(instance, %{identifiers: "abc", subscriptions: []})
    # Give the async task time to actually post before bypass tears down.
    Process.sleep(200)
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

    events = TelemetryCollector.events(collector)
    assert [{[:embassy_rails, :rpc, :exception], _measurements, meta}] = events
    assert meta.kind == :connect_error
  end
end
