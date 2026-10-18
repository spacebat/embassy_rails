defmodule Embassy.Rails.UpgradeTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias Embassy.Rails.Upgrade
  alias Embassy.Rails.Test.TelemetryCollector

  setup do
    start_supervised!({Finch, name: Embassy.Rails.UpgradeTest.Finch})
    start_supervised!({Phoenix.PubSub, name: Embassy.Rails.UpgradeTest.PubSub})

    start_supervised!(
      {Embassy, name: Embassy.Rails.UpgradeTest.Embassy, finch: Embassy.Rails.UpgradeTest.Finch}
    )

    start_supervised!(
      {Embassy.Rails,
       name: Embassy.Rails.UpgradeTest.Instance,
       embassy: Embassy.Rails.UpgradeTest.Embassy,
       pubsub: Embassy.Rails.UpgradeTest.PubSub,
       rpc: [transport: {:http, "http://example.test"}, secret: "shh"],
       broadcast: [source: :http, key: "bcast"]}
    )

    opts = Upgrade.init(embassy_rails: Embassy.Rails.UpgradeTest.Instance)
    {:ok, opts: opts}
  end

  defp ws_conn(opts) do
    headers = Keyword.get(opts, :headers, [])
    path = Keyword.get(opts, :path, "/cable")

    base = conn(:get, path)

    # Inject 'host' directly into req_headers — Plug.Test rejects it via put_req_header,
    # but WebSockAdapter's upgrade validation reads it from req_headers.
    base
    |> Map.update!(:req_headers, &[{"host", "localhost"} | &1])
    |> Plug.Conn.put_req_header("upgrade", "websocket")
    |> Plug.Conn.put_req_header("connection", "upgrade")
    |> Plug.Conn.put_req_header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
    |> Plug.Conn.put_req_header("sec-websocket-version", "13")
    |> then(fn c ->
      Enum.reduce(headers, c, fn {k, v}, acc -> Plug.Conn.put_req_header(acc, k, v) end)
    end)
  end

  test "echoes actioncable-v1-json subprotocol when requested", %{opts: opts} do
    conn =
      ws_conn(headers: [{"sec-websocket-protocol", "actioncable-v1-json"}])
      |> Upgrade.call(opts)

    assert ["actioncable-v1-json"] = Plug.Conn.get_resp_header(conn, "sec-websocket-protocol")
    # Plug.Test's adapter does not store the upgrade in a way that is introspectable here;
    # the resp-header echo above is sufficient evidence that the upgrade path was taken.
  end

  test "the WebSocket idle timeout defaults to 60s", %{opts: opts} do
    conn =
      ws_conn(headers: [{"sec-websocket-protocol", "actioncable-v1-json"}])
      |> Upgrade.call(opts)

    assert idle_timeout(conn) == 60_000
  end

  test "the instance's idle_timeout sets the WebSocket idle timeout" do
    start_supervised!(
      {Embassy.Rails,
       name: __MODULE__.IdleInstance,
       embassy: Embassy.Rails.UpgradeTest.Embassy,
       pubsub: Embassy.Rails.UpgradeTest.PubSub,
       rpc: [transport: {:http, "http://example.test"}, secret: "shh"],
       broadcast: [source: :http, key: "bcast"],
       idle_timeout: 5_000},
      id: :idle_instance
    )

    conn =
      ws_conn(headers: [{"sec-websocket-protocol", "actioncable-v1-json"}])
      |> Upgrade.call(Upgrade.init(embassy_rails: __MODULE__.IdleInstance))

    assert idle_timeout(conn) == 5_000
  end

  defp idle_timeout(conn) do
    assert [{:websocket, {_handler, _state, opts}}] = Plug.Test.sent_upgrades(conn)
    Keyword.fetch!(opts, :timeout)
  end

  test "echoes actioncable-v1-ext-json when requested", %{opts: opts} do
    conn =
      ws_conn(headers: [{"sec-websocket-protocol", "actioncable-v1-ext-json"}])
      |> Upgrade.call(opts)

    assert ["actioncable-v1-ext-json"] = Plug.Conn.get_resp_header(conn, "sec-websocket-protocol")
  end

  test "echoes the first matching subprotocol when several requested", %{opts: opts} do
    conn =
      ws_conn(headers: [{"sec-websocket-protocol", "graphql-ws, actioncable-v1-json"}])
      |> Upgrade.call(opts)

    assert ["actioncable-v1-json"] = Plug.Conn.get_resp_header(conn, "sec-websocket-protocol")
  end

  test "426 when no requested subprotocol matches", %{opts: opts} do
    {:ok, collector} = TelemetryCollector.attach([[:embassy_rails, :upgrade, :rejected]])

    conn =
      ws_conn(headers: [{"sec-websocket-protocol", "graphql-ws"}])
      |> Upgrade.call(opts)

    assert conn.status == 426

    events = TelemetryCollector.events(collector)
    assert [{[:embassy_rails, :upgrade, :rejected], _, meta}] = events
    assert meta.kind == :no_subprotocol
  end

  test "426 when no upgrade header is present", %{opts: opts} do
    {:ok, collector} = TelemetryCollector.attach([[:embassy_rails, :upgrade, :rejected]])

    conn = conn(:get, "/cable") |> Upgrade.call(opts)

    assert conn.status == 426

    events = TelemetryCollector.events(collector)
    assert [{[:embassy_rails, :upgrade, :rejected], _, meta}] = events
    assert meta.kind == :not_an_upgrade
  end

  test "configurable subprotocols list (graphql-ws use case)", %{} do
    custom_opts =
      Upgrade.init(
        embassy_rails: Embassy.Rails.UpgradeTest.Instance,
        subprotocols: ["graphql-ws"]
      )

    conn =
      ws_conn(headers: [{"sec-websocket-protocol", "graphql-ws"}])
      |> Upgrade.call(custom_opts)

    assert ["graphql-ws"] = Plug.Conn.get_resp_header(conn, "sec-websocket-protocol")
  end

  describe "extract_headers/2" do
    test "forwards configured headers case-insensitively" do
      conn =
        conn(:get, "/sync")
        |> Plug.Conn.put_req_header("x-auth-token", "tok")
        |> Plug.Conn.put_req_header("x-version", "1.77")
        |> Plug.Conn.put_req_header("authorization", "Bearer b")
        |> Plug.Conn.put_req_header("x-other", "drop me")

      headers = Upgrade.extract_headers(conn, ["X-AUTH-TOKEN", "x-version", "Authorization"])

      assert headers == %{
               "x-auth-token" => "tok",
               "x-version" => "1.77",
               "authorization" => "Bearer b"
             }
    end
  end

  describe "build_url/1" do
    test "uses wss when x-forwarded-proto is https" do
      conn =
        conn(:get, "/sync?token=abc")
        |> Map.update!(:req_headers, &[{"host", "ws.example"} | &1])
        |> Plug.Conn.put_req_header("x-forwarded-proto", "https")

      assert Upgrade.build_url(conn) == "wss://ws.example/sync?token=abc"
    end

    test "takes the client's scheme from a comma list in x-forwarded-proto" do
      conn =
        conn(:get, "/sync")
        |> Map.update!(:req_headers, &[{"host", "ws.example"} | &1])
        |> Plug.Conn.put_req_header("x-forwarded-proto", "https, http")

      assert Upgrade.build_url(conn) == "wss://ws.example/sync"
    end

    test "uses wss when x-forwarded-ssl is on" do
      conn =
        conn(:get, "/sync")
        |> Map.update!(:req_headers, &[{"host", "ws.example"} | &1])
        |> Plug.Conn.put_req_header("x-forwarded-ssl", "on")

      assert Upgrade.build_url(conn) == "wss://ws.example/sync"
    end

    test "falls back to ws for plain http" do
      conn = conn(:get, "/sync") |> Map.update!(:req_headers, &[{"host", "ws.example"} | &1])
      assert Upgrade.build_url(conn) == "ws://ws.example/sync"
    end
  end

  test "instance :headers option is merged with defaults in the snapshot" do
    start_supervised!(
      {Embassy.Rails,
       name: __MODULE__.HeadersInstance,
       embassy: Embassy.Rails.UpgradeTest.Embassy,
       pubsub: Embassy.Rails.UpgradeTest.PubSub,
       rpc: [transport: {:http, "http://example.test"}, secret: "shh"],
       broadcast: [source: :http, key: "bcast"],
       headers: ["X-Auth-Token"]},
      id: :headers_instance
    )

    headers = Embassy.Rails.Config.snapshot(__MODULE__.HeadersInstance).headers
    assert "x-auth-token" in headers
    assert "cookie" in headers
  end
end
