defmodule Embassy.Rails.PresenceApiPlug do
  @moduledoc """
  anycable-go compatible presence API: `GET <mount>/:stream/users`.

      forward "/api/presence", Embassy.Rails.PresenceApiPlug,
        embassy_rails: MyApp.Embassy.Rails

  The stream segment is percent-decoded (a literal `+` stays a `+`, as in
  anycable-go), so GlobalID-based stream names round-trip. The response is

      {"type": "info", "total": 2, "records": [{"id": "u1", "info": {...}}, ...]}

  Bearer auth against `presence: [api_secret: ...]`; with no secret the
  endpoint is open. When `presence: [upstream: base_url]` is set (an
  anycable-go still serving part of the traffic), its records are fetched
  with the same bearer and unioned by id. If the upstream fails the
  response is 503: a partial set would make callers think connected
  devices are gone.

  Other statuses follow anycable-go: 404 for another path, 422 for a
  method other than GET, 501 when presence is disabled.
  """

  @behaviour Plug

  import Plug.Conn

  alias Embassy.Rails.{Config, Presence}

  @upstream_timeout 3_000

  @impl Plug
  def init(opts), do: %{embassy_rails: Keyword.fetch!(opts, :embassy_rails)}

  @impl Plug
  def call(%{path_info: [encoded, "users"]} = conn, %{embassy_rails: instance}) do
    snap = Config.snapshot(instance)

    cond do
      conn.method != "GET" -> reply(conn, 422, "unprocessable entity")
      snap.presence == nil -> reply(conn, 501, "presence is disabled")
      not authorized?(conn, snap.presence.api_secret) -> reply(conn, 401, "unauthorized")
      true -> users(conn, instance, snap, encoded)
    end
  end

  def call(conn, _opts), do: reply(conn, 404, "not found")

  defp users(conn, instance, snap, encoded) do
    case decode_stream(encoded) do
      {:ok, stream} -> union(conn, instance, snap, stream, encoded)
      :error -> reply(conn, 404, "not found")
    end
  end

  defp decode_stream(encoded) do
    {:ok, URI.decode(encoded)}
  rescue
    ArgumentError -> :error
  end

  defp union(conn, instance, snap, stream, encoded) do
    {:ok, %{"records" => local}} = Presence.info(instance, stream, true)

    case upstream(snap, encoded) do
      :none -> respond(conn, local)
      {:ok, remote} -> respond(conn, Enum.uniq_by(local ++ remote, & &1["id"]))
      {:error, _} -> reply(conn, 503, "presence upstream unavailable")
    end
  end

  defp authorized?(_conn, nil), do: true

  defp authorized?(conn, secret) do
    Plug.Crypto.secure_compare(
      Enum.join(get_req_header(conn, "authorization")),
      "Bearer " <> secret
    )
  end

  defp upstream(%{presence: %{upstream: nil}}, _encoded), do: :none

  defp upstream(%{presence: %{upstream: base, api_secret: secret}, finch: finch}, encoded) do
    headers = if secret, do: [{"authorization", "Bearer " <> secret}], else: []
    url = "#{String.trim_trailing(base, "/")}/api/presence/#{encoded}/users"

    with {:ok, %Finch.Response{status: 200, body: body}} <-
           Finch.request(Finch.build(:get, url, headers), finch,
             receive_timeout: @upstream_timeout
           ),
         {:ok, %{"records" => records}} when is_list(records) <- Jason.decode(body) do
      {:ok, for(%{"id" => id} = record <- records, do: %{"id" => id, "info" => record["info"]})}
    else
      other -> {:error, other}
    end
  end

  defp respond(conn, records) do
    body = %{type: "info", total: length(records), records: records}

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(body))
    |> halt()
  end

  defp reply(conn, status, message) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(status, message)
    |> halt()
  end
end
