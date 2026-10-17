defmodule Embassy.Rails.BroadcastPlugTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias Embassy.Rails.{Broadcast, BroadcastPlug}
  alias Embassy.Rails.Test.TelemetryCollector

  setup do
    start_supervised!({Finch, name: Embassy.Rails.BroadcastPlugTest.Finch})
    start_supervised!({Phoenix.PubSub, name: Embassy.Rails.BroadcastPlugTest.PubSub})

    start_supervised!(
      {Embassy,
       name: Embassy.Rails.BroadcastPlugTest.Embassy, finch: Embassy.Rails.BroadcastPlugTest.Finch}
    )

    start_supervised!(
      {Embassy.Rails,
       name: Embassy.Rails.BroadcastPlugTest.Instance,
       embassy: Embassy.Rails.BroadcastPlugTest.Embassy,
       pubsub: Embassy.Rails.BroadcastPlugTest.PubSub,
       rpc: [transport: {:http, "http://example.test"}, secret: "shh"],
       broadcast: [source: :http, key: "the-key"]}
    )

    opts = BroadcastPlug.init(embassy_rails: Embassy.Rails.BroadcastPlugTest.Instance)
    {:ok, opts: opts}
  end

  defp post_json(body, headers) do
    conn = conn(:post, "/_broadcast", Jason.encode!(body))

    Enum.reduce(headers, conn, fn {k, v}, c -> Plug.Conn.put_req_header(c, k, v) end)
    |> Plug.Conn.put_req_header("content-type", "application/json")
  end

  test "204 on a valid single-entry envelope with correct bearer", %{opts: opts} do
    Phoenix.PubSub.subscribe(Embassy.Rails.BroadcastPlugTest.PubSub, Broadcast.topic("chat_5"))

    body = %{"stream" => "chat_5", "data" => ~s({"text":"hi"})}
    conn = post_json(body, [{"authorization", "Bearer the-key"}]) |> BroadcastPlug.call(opts)

    assert conn.status == 204
    assert_receive {:cable_broadcast, "chat_5", %{data: ~s({"text":"hi"})}}, 100
  end

  test "204 on a batched body (AnyCable batch shape: bare JSON array)", %{opts: opts} do
    Phoenix.PubSub.subscribe(Embassy.Rails.BroadcastPlugTest.PubSub, Broadcast.topic("a"))
    Phoenix.PubSub.subscribe(Embassy.Rails.BroadcastPlugTest.PubSub, Broadcast.topic("b"))

    conn =
      conn(:post, "/_broadcast", ~s([{"stream":"a","data":"1"},{"stream":"b","data":"2"}]))
      |> Plug.Conn.put_req_header("authorization", "Bearer the-key")
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> BroadcastPlug.call(opts)

    assert conn.status == 204
    assert_receive {:cable_broadcast, "a", %{data: "1"}}, 100
    assert_receive {:cable_broadcast, "b", %{data: "2"}}, 100
  end

  test "dispatches with :cluster scope (Rails posts to one node)", %{opts: opts} do
    {:ok, collector} = TelemetryCollector.attach([[:embassy_rails, :broadcast, :received]])
    body = %{"stream" => "scoped", "data" => "1"}

    post_json(body, [{"authorization", "Bearer the-key"}]) |> BroadcastPlug.call(opts)

    assert [%{scope: :cluster}] =
             for(
               {_event, _meas, %{stream: "scoped"} = meta} <- TelemetryCollector.events(collector),
               do: meta
             )
  end

  test "401 on missing authorization header", %{opts: opts} do
    body = %{"stream" => "chat", "data" => "hi"}
    conn = post_json(body, []) |> BroadcastPlug.call(opts)

    assert conn.status == 401
  end

  test "401 on wrong bearer token", %{opts: opts} do
    body = %{"stream" => "chat", "data" => "hi"}
    conn = post_json(body, [{"authorization", "Bearer wrong"}]) |> BroadcastPlug.call(opts)

    assert conn.status == 401
  end

  test "400 on malformed JSON", %{opts: opts} do
    conn =
      conn(:post, "/_broadcast", "not json")
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> Plug.Conn.put_req_header("authorization", "Bearer the-key")
      |> BroadcastPlug.call(opts)

    assert conn.status == 400
  end

  test "emits :rejected telemetry on bad bearer", %{opts: opts} do
    {:ok, collector} = TelemetryCollector.attach([[:embassy_rails, :broadcast, :rejected]])

    body = %{"stream" => "chat", "data" => "hi"}
    post_json(body, [{"authorization", "Bearer wrong"}]) |> BroadcastPlug.call(opts)

    events = TelemetryCollector.events(collector)
    assert [{[:embassy_rails, :broadcast, :rejected], _, meta}] = events
    assert meta.kind == :bad_bearer
  end
end
