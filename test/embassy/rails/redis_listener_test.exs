defmodule Embassy.Rails.RedisListenerTest do
  use ExUnit.Case, async: true

  alias Embassy.Rails.Broadcast
  alias Embassy.Rails.RedisListener

  describe "decode_anycable/1" do
    test "decodes a well-formed envelope" do
      payload = ~s({"stream":"chat:1","data":"{\\"x\\":1}"})

      assert RedisListener.decode_anycable(payload) ==
               {:ok, "chat:1", %{data: ~s({"x":1}), exclude_socket: nil}}
    end

    test "extracts meta.exclude_socket when present" do
      payload = ~s({"stream":"chat:1","data":"hi","meta":{"exclude_socket":"sock-9"}})

      assert RedisListener.decode_anycable(payload) ==
               {:ok, "chat:1", %{data: "hi", exclude_socket: "sock-9"}}
    end

    test "returns :error on malformed JSON" do
      assert RedisListener.decode_anycable("not json") == :error
    end

    test "returns :error when stream is missing" do
      assert RedisListener.decode_anycable(~s({"data":"hi"})) == :error
    end

    test "returns :error when stream is not a string" do
      assert RedisListener.decode_anycable(~s({"stream":42,"data":"hi"})) == :error
    end
  end

  describe "handle_info :message" do
    setup do
      start_supervised!({Finch, name: __MODULE__.Finch})
      start_supervised!({Phoenix.PubSub, name: __MODULE__.PubSub})
      start_supervised!({Embassy, name: __MODULE__.Embassy, finch: __MODULE__.Finch})

      start_supervised!(
        {Embassy.Rails,
         name: __MODULE__.Instance,
         embassy: __MODULE__.Embassy,
         pubsub: __MODULE__.PubSub,
         rpc: [transport: {:http, "http://example.test"}, secret: "shh"],
         broadcast: [source: :http, key: "bcast"]}
      )

      {:ok, instance: __MODULE__.Instance}
    end

    test "decodes a message and fans it out via dispatch", %{instance: instance} do
      Phoenix.PubSub.subscribe(__MODULE__.PubSub, Broadcast.topic("chat:1"))

      state = %{
        instance: instance,
        channel: "__anycable__",
        conn: nil,
        ref: nil,
        status: :connected
      }

      msg =
        {:redix_pubsub, self(), make_ref(), :message,
         %{channel: "__anycable__", payload: ~s({"stream":"chat:1","data":"hi"})}}

      assert {:noreply, ^state} = RedisListener.handle_info(msg, state)
      assert_receive {:cable_broadcast, "chat:1", %{data: "hi"}}, 100
    end
  end

  describe "handle_info lifecycle" do
    alias Embassy.Rails.Test.TelemetryCollector

    test "emits :connected telemetry on initial :subscribed" do
      {:ok, collector} =
        TelemetryCollector.attach([[:embassy_rails, :broadcast, :redis, :connected]])

      state = %{instance: :inst, channel: "__anycable__", conn: nil, ref: nil, status: :initial}

      msg = {:redix_pubsub, self(), make_ref(), :subscribed, %{channel: "__anycable__"}}
      assert {:noreply, %{status: :connected}} = RedisListener.handle_info(msg, state)

      assert [{[:embassy_rails, :broadcast, :redis, :connected], _meas, meta}] =
               TelemetryCollector.events(collector)

      assert meta.instance == :inst
      assert meta.channel == "__anycable__"
    end

    test "emits :reconnected telemetry on :subscribed after a disconnect" do
      {:ok, collector} =
        TelemetryCollector.attach([[:embassy_rails, :broadcast, :redis, :reconnected]])

      state = %{
        instance: :inst,
        channel: "__anycable__",
        conn: nil,
        ref: nil,
        status: :disconnected
      }

      msg = {:redix_pubsub, self(), make_ref(), :subscribed, %{channel: "__anycable__"}}
      assert {:noreply, %{status: :connected}} = RedisListener.handle_info(msg, state)

      assert [{[:embassy_rails, :broadcast, :redis, :reconnected], _meas, meta}] =
               TelemetryCollector.events(collector)

      assert meta.instance == :inst
      assert meta.channel == "__anycable__"
    end

    test "emits :disconnected telemetry on :disconnected" do
      {:ok, collector} =
        TelemetryCollector.attach([[:embassy_rails, :broadcast, :redis, :disconnected]])

      state = %{instance: :inst, channel: "__anycable__", conn: nil, ref: nil, status: :connected}

      msg =
        {:redix_pubsub, self(), make_ref(), :disconnected,
         %{error: %Redix.ConnectionError{reason: :closed}}}

      assert {:noreply, %{status: :disconnected}} = RedisListener.handle_info(msg, state)

      assert [{[:embassy_rails, :broadcast, :redis, :disconnected], _meas, meta}] =
               TelemetryCollector.events(collector)

      assert meta.instance == :inst
    end

    test "includes reconnect_timeout in the disconnect log when provided" do
      state = %{instance: :inst, channel: "__anycable__", conn: nil, ref: nil, status: :connected}

      msg =
        {:redix_pubsub, self(), make_ref(), :disconnected,
         %{error: %Redix.ConnectionError{reason: :closed}, reconnect_timeout: 1000}}

      assert {:noreply, %{status: :disconnected}} = RedisListener.handle_info(msg, state)
    end
  end

  describe "handle_info :message with a bad envelope" do
    alias Embassy.Rails.Test.TelemetryCollector

    test "drops the message and emits :exception telemetry" do
      {:ok, collector} =
        TelemetryCollector.attach([[:embassy_rails, :broadcast, :redis, :exception]])

      state = %{instance: :inst, channel: "c", conn: nil, ref: nil, status: :connected}

      msg = {:redix_pubsub, self(), make_ref(), :message, %{channel: "c", payload: "not json"}}
      assert {:noreply, ^state} = RedisListener.handle_info(msg, state)

      assert [{[:embassy_rails, :broadcast, :redis, :exception], _meas, meta}] =
               TelemetryCollector.events(collector)

      assert meta.kind == :bad_envelope
      assert meta.instance == :inst
    end
  end
end
