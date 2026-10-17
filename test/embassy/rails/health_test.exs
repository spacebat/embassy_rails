defmodule Embassy.Rails.HealthTest do
  use ExUnit.Case, async: false

  alias Embassy.Rails.{Drain, Health}

  setup do
    bypass = Bypass.open()
    start_supervised!({Finch, name: __MODULE__.Finch})
    start_supervised!({Phoenix.PubSub, name: __MODULE__.PubSub})
    start_supervised!({Embassy, name: __MODULE__.Embassy, finch: __MODULE__.Finch})

    start_supervised!(
      {Embassy.Rails,
       name: __MODULE__.I,
       embassy: __MODULE__.Embassy,
       pubsub: __MODULE__.PubSub,
       rpc: [transport: {:http, "http://localhost:#{bypass.port}"}, secret: "s"],
       broadcast: [source: :http, key: "k"]}
    )

    {:ok, bypass: bypass}
  end

  test "ready when the HTTP RPC answers, not ready when it is down", %{bypass: bypass} do
    Bypass.stub(bypass, "HEAD", "/", fn conn -> Plug.Conn.resp(conn, 404, "") end)
    assert Health.ready?(__MODULE__.I)
    Bypass.down(bypass)
    refute Health.ready?(__MODULE__.I)
  end

  test "not ready while draining", %{bypass: bypass} do
    Bypass.stub(bypass, "HEAD", "/", fn conn -> Plug.Conn.resp(conn, 404, "") end)
    start_supervised!({Drain, instance: __MODULE__.I, window_ms: 10, batches: 1})
    assert Health.ready?(__MODULE__.I)

    stop_supervised!({Drain, __MODULE__.I})
    refute Health.ready?(__MODULE__.I)
  end
end
