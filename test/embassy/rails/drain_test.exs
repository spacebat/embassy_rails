defmodule Embassy.Rails.DrainTest do
  use ExUnit.Case, async: false

  alias Embassy.Rails.Drain

  setup do
    start_supervised!({Finch, name: __MODULE__.Finch})
    start_supervised!({Phoenix.PubSub, name: __MODULE__.PubSub})
    start_supervised!({Embassy, name: __MODULE__.Embassy, finch: __MODULE__.Finch})

    start_supervised!(
      {Embassy.Rails,
       name: __MODULE__.I,
       embassy: __MODULE__.Embassy,
       pubsub: __MODULE__.PubSub,
       rpc: [transport: {:http, "http://example.test"}, secret: "s"],
       broadcast: [source: :http, key: "k"]}
    )

    :ok
  end

  test "on shutdown marks draining and sends every connection the drain message in batches" do
    test_pid = self()

    for i <- 1..5 do
      spawn(fn ->
        Registry.register(Embassy.Rails.connections_name(__MODULE__.I), :conn, nil)
        send(test_pid, {:registered, i})

        receive do
          :embassy_rails_drain ->
            send(test_pid, {:drained, i, System.monotonic_time(:millisecond)})
        end
      end)
    end

    for i <- 1..5, do: assert_receive({:registered, ^i})
    drain = start_supervised!({Drain, instance: __MODULE__.I, window_ms: 200, batches: 5})
    refute Drain.draining?(__MODULE__.I)

    stop_supervised!({Drain, __MODULE__.I})
    refute Process.alive?(drain)

    times =
      for i <- 1..5 do
        assert_receive {:drained, ^i, t}
        t
      end

    assert Enum.max(times) - Enum.min(times) >= 100
    assert Drain.draining?(__MODULE__.I)
  end

  test "a restarted instance is not left draining" do
    start_supervised!({Drain, instance: __MODULE__.I, window_ms: 10, batches: 1})
    stop_supervised!({Drain, __MODULE__.I})
    assert Drain.draining?(__MODULE__.I)

    stop_supervised!(Embassy.Rails.child_spec(name: __MODULE__.I).id)

    start_supervised!(
      {Embassy.Rails,
       name: __MODULE__.I,
       embassy: __MODULE__.Embassy,
       pubsub: __MODULE__.PubSub,
       rpc: [transport: {:http, "http://example.test"}, secret: "s"],
       broadcast: [source: :http, key: "k"]}
    )

    refute Drain.draining?(__MODULE__.I)
  end

  test "the child spec's shutdown outlasts the window" do
    spec = Drain.child_spec(instance: __MODULE__.I, window_ms: 1_000)
    assert spec.shutdown > 1_000
  end
end
