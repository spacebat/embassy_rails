defmodule Embassy.Rails.BroadcastPlug do
  @moduledoc """
  HTTP endpoint Plug for AnyCable's broadcast adapter.

  Mount in your router on `POST /_broadcast`:

      post "/_broadcast", Embassy.Rails.BroadcastPlug,
        embassy_rails: MyApp.Embassy.Rails

  Or via `forward`:

      forward "/_broadcast", Embassy.Rails.BroadcastPlug,
        embassy_rails: MyApp.Embassy.Rails

  Rails publishes via `AnyCable::Rails.broadcast/2` which posts a
  JSON envelope here. The plug verifies a shared bearer token, parses
  the envelope, and fans the entries out via `Phoenix.PubSub` to every
  `Embassy.Rails.Connection` subscribed to the matching stream.
  """

  @behaviour Plug

  import Plug.Conn
  require Logger

  alias Embassy.Rails.{Broadcast, BroadcastEnvelope, Config}

  @impl Plug
  def init(opts) do
    %{embassy_rails: Keyword.fetch!(opts, :embassy_rails)}
  end

  @impl Plug
  def call(conn, %{embassy_rails: instance}) do
    snap = Config.snapshot(instance)
    expected = "Bearer " <> snap.broadcast[:key]

    case authorization(conn) do
      ^expected -> handle_authorized(conn, instance)
      _ -> reject(conn, instance)
    end
  end

  defp authorization(conn) do
    case get_req_header(conn, "authorization") do
      [v | _] -> v
      _ -> ""
    end
  end

  defp handle_authorized(conn, instance) do
    {:ok, body, conn} = read_body(conn)

    case BroadcastEnvelope.decode(body) do
      {:ok, items} ->
        BroadcastEnvelope.dispatch_items(instance, items, Broadcast.scope(:http))
        send_resp(conn, 204, "")

      {:error, :not_an_envelope} ->
        send_resp(conn, 204, "")

      {:error, :invalid_json = reason} ->
        Logger.warning("Embassy.Rails.BroadcastPlug: bad JSON body")

        :telemetry.execute(
          [:embassy_rails, :broadcast, :exception],
          %{monotonic_time: System.monotonic_time()},
          %{instance: instance, kind: :bad_envelope, reason: reason}
        )

        send_resp(conn, 400, "Bad Request: malformed JSON")
    end
  end

  defp reject(conn, instance) do
    Logger.warning("Embassy.Rails.BroadcastPlug: rejected request (bad bearer)")

    :telemetry.execute(
      [:embassy_rails, :broadcast, :rejected],
      %{monotonic_time: System.monotonic_time()},
      %{instance: instance, kind: :bad_bearer}
    )

    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(401, "Unauthorized")
  end
end
