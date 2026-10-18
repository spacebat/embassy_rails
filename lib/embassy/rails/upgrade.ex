defmodule Embassy.Rails.Upgrade do
  @moduledoc """
  WebSocket upgrade plug that hands off to `Embassy.Rails.Connection`.

  Negotiates a subprotocol from the client's `sec-websocket-protocol`
  header against a configured list (default: `["actioncable-v1-json"]`).
  Echoes the first match back via the response header, then calls
  `WebSockAdapter.upgrade/4` with the initial connection state.

  ## Usage

      forward "/cable", Embassy.Rails.Upgrade,
        embassy_rails: MyApp.Embassy.Rails

  Or with a custom subprotocol list:

      forward "/cable", Embassy.Rails.Upgrade,
        embassy_rails: MyApp.Embassy.Rails,
        subprotocols: ["actioncable-v1-json", "graphql-ws"]

  Mount this plug on WS-only routes. Non-WS requests get a 426, and
  upgrades while `Embassy.Rails.Drain` is draining the instance get a 503.
  """

  @behaviour Plug

  import Plug.Conn

  require Logger

  # ext-json carries AnyCable's presence commands; plain json clients are
  # unaffected.
  @default_subprotocols ["actioncable-v1-ext-json", "actioncable-v1-json"]

  @impl Plug
  def init(opts) do
    %{
      embassy_rails: Keyword.fetch!(opts, :embassy_rails),
      subprotocols: Keyword.get(opts, :subprotocols, @default_subprotocols)
    }
  end

  @impl Plug
  def call(conn, %{embassy_rails: instance, subprotocols: subprotocols}) do
    cond do
      not websocket_upgrade?(conn) ->
        reject(conn, instance, :not_an_upgrade, [])

      # Send the client to another node rather than accept a connection
      # the drain has already passed.
      Embassy.Rails.Drain.draining?(instance) ->
        reject(conn, instance, :draining, [])

      true ->
        negotiate(conn, instance, subprotocols)
    end
  end

  defp negotiate(conn, instance, subprotocols) do
    requested = requested_subprotocols(conn)

    case Enum.find(subprotocols, &(&1 in requested)) do
      nil -> reject(conn, instance, :no_subprotocol, requested)
      chosen -> do_upgrade(conn, instance, chosen)
    end
  end

  defp websocket_upgrade?(conn) do
    Enum.any?(conn.req_headers, fn
      {"upgrade", value} -> String.downcase(value) == "websocket"
      _ -> false
    end)
  end

  defp requested_subprotocols(conn) do
    conn
    |> get_req_header("sec-websocket-protocol")
    |> Enum.flat_map(&String.split(&1, ~r/\s*,\s*/, trim: true))
  end

  defp do_upgrade(conn, instance, chosen) do
    snap = Embassy.Rails.Config.snapshot(instance)

    state = %{
      instance: instance,
      socket_id: random_id(),
      url: build_url(conn),
      headers: extract_headers(conn, snap.headers),
      identifiers: nil,
      cstate: %{},
      istate: %{},
      streams: %{},
      presence_streams: %{},
      slow_since: nil
    }

    conn
    |> put_resp_header("sec-websocket-protocol", chosen)
    |> WebSockAdapter.upgrade(Embassy.Rails.Connection, state, timeout: snap.tuning.idle_timeout)
    |> halt()
  end

  defp reject(conn, instance, kind, requested) do
    Logger.warning(
      "Embassy.Rails.Upgrade: rejected upgrade (#{kind}); requested=#{inspect(requested)}"
    )

    :telemetry.execute(
      [:embassy_rails, :upgrade, :rejected],
      %{monotonic_time: System.monotonic_time()},
      %{instance: instance, kind: kind, requested_subprotocols: requested}
    )

    {status, body} =
      if kind == :draining,
        do: {503, "Service Unavailable: draining"},
        else: {426, "Upgrade Required: WebSocket upgrade not accepted"}

    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(status, body)
    |> halt()
  end

  defp random_id,
    do: "phx-" <> (16 |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false))

  @doc false
  # The client-facing URL. Behind a TLS-terminating proxy conn.scheme is
  # :http, so the proxy's x-forwarded-proto / x-forwarded-ssl decide.
  def build_url(conn) do
    scheme = if secure?(conn), do: "wss", else: "ws"
    host = conn_host(conn)
    qs = if conn.query_string == "", do: "", else: "?" <> conn.query_string
    "#{scheme}://#{host}#{conn.request_path}#{qs}"
  end

  defp conn_host(conn) do
    case List.keyfind(conn.req_headers, "host", 0) do
      {"host", v} -> v
      _ -> conn.host
    end
  end

  defp secure?(conn) do
    case get_req_header(conn, "x-forwarded-proto") do
      # Each proxy hop appends, so the first value is the client's.
      [proto | _] -> client_proto(proto) in ["https", "wss"]
      [] -> conn.scheme == :https or get_req_header(conn, "x-forwarded-ssl") == ["on"]
    end
  end

  defp client_proto(proto) do
    proto |> String.split(",", parts: 2) |> hd() |> String.trim() |> String.downcase()
  end

  @doc false
  # Request headers forwarded to the RPC Env, matched case-insensitively.
  def extract_headers(conn, allowed) do
    allowed = MapSet.new(allowed, &String.downcase/1)
    for {k, v} <- conn.req_headers, MapSet.member?(allowed, k), into: %{}, do: {k, v}
  end
end
