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

  describe "with a Redis broadcast source" do
    for source <- [:redis, :redisx] do
      @tag :capture_log
      test "not ready while the #{source} listener cannot reach Redis", %{bypass: bypass} do
        Bypass.stub(bypass, "HEAD", "/", fn conn -> Plug.Conn.resp(conn, 404, "") end)
        instance = Module.concat(__MODULE__, unquote(source))

        start_supervised!(
          {Embassy.Rails,
           name: instance,
           embassy: __MODULE__.Embassy,
           pubsub: __MODULE__.PubSub,
           rpc: [transport: {:http, "http://localhost:#{bypass.port}"}, secret: "s"],
           broadcast: [source: unquote(source), redis: "redis://127.0.0.1:1"]},
          id: instance
        )

        refute Health.ready?(instance)
      end
    end
  end

  describe "probe_address/1" do
    test "parses every gRPC target form to the first address" do
      for {target, expected} <- [
            {"127.0.0.1:50051", {~c"127.0.0.1", 50051}},
            {"rpc.svc:50051", {~c"rpc.svc", 50051}},
            {"dns:rpc.svc:50051", {~c"rpc.svc", 50051}},
            {"dns:///rpc.svc:50051", {~c"rpc.svc", 50051}},
            {"dns://8.8.8.8/rpc.svc:50051", {~c"rpc.svc", 50051}},
            {"ipv4:10.0.0.1:50051", {~c"10.0.0.1", 50051}},
            {"ipv4:10.0.0.1:50051,10.0.0.2:50051", {~c"10.0.0.1", 50051}},
            {"ipv6:[::1]:50051", {~c"::1", 50051}},
            {"[::1]:50051", {~c"::1", 50051}},
            {"unix:/tmp/rpc.sock", {{:local, "/tmp/rpc.sock"}, 0}},
            {"unix:///tmp/rpc.sock", {{:local, "/tmp/rpc.sock"}, 0}}
          ] do
        assert Health.probe_address(target) == {:ok, expected}, target
      end
    end

    test "is :error for a target it cannot parse" do
      for target <- ["rpc.svc", "rpc.svc:http", "ipv4:", ""] do
        assert Health.probe_address(target) == :error, target
      end
    end
  end

  test "the TCP probe reaches an IPv6 loopback endpoint" do
    case :gen_tcp.listen(0, [:inet6, ip: {0, 0, 0, 0, 0, 0, 0, 1}]) do
      {:ok, listener} ->
        {:ok, port} = :inet.port(listener)
        assert Health.tcp_ok?("ipv6:[::1]:#{port}")
        :gen_tcp.close(listener)

      {:error, _} ->
        :ok
    end
  end
end
