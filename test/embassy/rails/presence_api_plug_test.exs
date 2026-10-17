defmodule Embassy.Rails.PresenceApiPlugTest do
  use ExUnit.Case, async: true
  import Plug.Test

  alias Embassy.Rails.{Presence, PresenceApiPlug}

  setup do
    start_supervised!({Finch, name: __MODULE__.Finch})
    start_supervised!({Phoenix.PubSub, name: __MODULE__.PubSub})
    start_supervised!({Embassy, name: __MODULE__.Embassy, finch: __MODULE__.Finch})
    :ok
  end

  defp start_instance(presence) do
    name = Module.concat(__MODULE__, "I#{System.unique_integer([:positive])}")

    start_supervised!(
      {Embassy.Rails,
       name: name,
       embassy: __MODULE__.Embassy,
       pubsub: __MODULE__.PubSub,
       rpc: [transport: {:http, "http://example.test"}, secret: "s"],
       broadcast: [source: :http, key: "k"],
       presence: presence},
      id: name
    )

    name
  end

  # Simulates `forward "/api/presence", PresenceApiPlug, ...`: Phoenix hands
  # the plug the remaining, still percent-encoded, path segments.
  defp request(instance, method, segments, auth \\ "Bearer sekret") do
    opts = PresenceApiPlug.init(embassy_rails: instance)

    conn(method, "/api/presence/" <> Enum.join(segments, "/"))
    |> Map.put(:script_name, ["api", "presence"])
    |> Map.put(:path_info, segments)
    |> then(fn conn ->
      if auth, do: Plug.Conn.put_req_header(conn, "authorization", auth), else: conn
    end)
    |> PresenceApiPlug.call(opts)
  end

  defp get_users(instance, encoded, auth \\ "Bearer sekret"),
    do: request(instance, :get, [encoded, "users"], auth)

  test "returns anycable-go shaped records for an encoded stream name" do
    instance = start_instance(api_secret: "sekret")
    stream = "time_clock_app:schedule:Z2lkOi8vYXBpL1NjaGVkdWxlLzE="
    :ok = Presence.join(instance, stream, "sid1", "dev-1", nil)

    conn = get_users(instance, URI.encode_www_form(stream))
    assert conn.status == 200
    assert ["application/json" <> _] = Plug.Conn.get_resp_header(conn, "content-type")

    assert Jason.decode!(conn.resp_body) ==
             %{"type" => "info", "total" => 1, "records" => [%{"id" => "dev-1", "info" => nil}]}
  end

  test "decodes percent escapes only, keeping a literal + (as anycable-go does)" do
    instance = start_instance(api_secret: "sekret")
    :ok = Presence.join(instance, "chat/1+a%b", "sid1", "u1", %{"n" => 1})

    conn = get_users(instance, "chat%2F1+a%25b")

    assert %{"total" => 1, "records" => [%{"id" => "u1", "info" => %{"n" => 1}}]} =
             Jason.decode!(conn.resp_body)
  end

  test "401 on a wrong or missing bearer" do
    instance = start_instance(api_secret: "sekret")
    assert get_users(instance, "s1", "Bearer nope").status == 401
    assert get_users(instance, "s1", nil).status == 401
  end

  test "no auth needed when api_secret is nil" do
    instance = start_instance([])
    assert get_users(instance, "s1", nil).status == 200
  end

  test "404 for another path, 422 for a non-GET" do
    instance = start_instance(api_secret: "sekret")
    assert request(instance, :get, ["chat", "1"]).status == 404
    assert request(instance, :post, ["x", "users"]).status == 422
  end

  test "a malformed percent escape does not crash" do
    instance = start_instance([])
    # URI.decode/1 keeps a malformed escape verbatim on recent Elixir and
    # raises on older ones (answered with 404).
    assert get_users(instance, "bad%zz", nil).status in [200, 404]
  end

  test "501 when presence is disabled" do
    instance = start_instance(false)
    assert get_users(instance, "s1", nil).status == 501
  end

  test "unions with the upstream and dedupes by id" do
    bypass = Bypass.open()
    instance = start_instance(api_secret: "sekret", upstream: "http://localhost:#{bypass.port}")
    :ok = Presence.join(instance, "s1", "sid1", "a", nil)

    Bypass.expect_once(bypass, "GET", "/api/presence/s1/users", fn conn ->
      assert Plug.Conn.get_req_header(conn, "authorization") == ["Bearer sekret"]
      Plug.Conn.resp(conn, 200, ~s({"type":"info","total":2,"records":[{"id":"a"},{"id":"b"}]}))
    end)

    body = Jason.decode!(get_users(instance, "s1").resp_body)
    assert body["total"] == 2
    assert body["records"] |> Enum.map(& &1["id"]) |> Enum.sort() == ["a", "b"]
  end

  test "503 when the upstream fails, never a partial list" do
    bypass = Bypass.open()
    instance = start_instance(api_secret: "sekret", upstream: "http://localhost:#{bypass.port}")
    Bypass.down(bypass)
    assert get_users(instance, "s1").status == 503
  end

  test "503 when the upstream answers with an error status" do
    bypass = Bypass.open()
    instance = start_instance(api_secret: "sekret", upstream: "http://localhost:#{bypass.port}")
    Bypass.expect_once(bypass, "GET", "/api/presence/s1/users", &Plug.Conn.resp(&1, 500, "boom"))
    assert get_users(instance, "s1").status == 503
  end
end
