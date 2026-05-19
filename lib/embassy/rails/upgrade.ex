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

  Mount this plug on WS-only routes. Non-WS requests get a 426.
  """

  @behaviour Plug

  import Plug.Conn

  require Logger

  @default_subprotocols ["actioncable-v1-json"]
  @forwarded_headers ~w(cookie user-agent origin x-forwarded-for x-real-ip)

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

      true ->
        requested = requested_subprotocols(conn)

        case Enum.find(subprotocols, &(&1 in requested)) do
          nil -> reject(conn, instance, :no_subprotocol, requested)
          chosen -> do_upgrade(conn, instance, chosen)
        end
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
    state = %{
      instance: instance,
      socket_id: random_id(),
      url: build_url(conn),
      headers: extract_headers(conn),
      identifiers: nil,
      cstate: %{},
      istate: %{},
      streams: %{}
    }

    conn
    |> put_resp_header("sec-websocket-protocol", chosen)
    |> WebSockAdapter.upgrade(Embassy.Rails.Connection, state, timeout: 60_000)
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

    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(426, "Upgrade Required: WebSocket upgrade not accepted")
    |> halt()
  end

  defp random_id,
    do: "phx-" <> (16 |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false))

  defp build_url(conn) do
    scheme = if conn.scheme == :https, do: "wss", else: "ws"
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

  defp extract_headers(conn) do
    for {k, v} <- conn.req_headers, k in @forwarded_headers, into: %{}, do: {k, v}
  end
end
