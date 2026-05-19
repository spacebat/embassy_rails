defmodule Embassy.Rails.BroadcastTest do
  use ExUnit.Case, async: true

  alias Embassy.Rails.Broadcast

  setup do
    start_supervised!({Finch, name: Embassy.Rails.BroadcastTest.Finch})
    start_supervised!({Phoenix.PubSub, name: Embassy.Rails.BroadcastTest.PubSub})

    start_supervised!(
      {Embassy,
       name: Embassy.Rails.BroadcastTest.Embassy, finch: Embassy.Rails.BroadcastTest.Finch}
    )

    start_supervised!(
      {Embassy.Rails,
       name: Embassy.Rails.BroadcastTest.Instance,
       embassy: Embassy.Rails.BroadcastTest.Embassy,
       pubsub: Embassy.Rails.BroadcastTest.PubSub,
       rpc: [transport: {:http, "http://example.test"}, secret: "shh"],
       broadcast: [source: :http, key: "bcast"]}
    )

    {:ok, instance: Embassy.Rails.BroadcastTest.Instance}
  end

  test "topic/1 prefixes the stream id" do
    assert Broadcast.topic("chat_5") == "embassy_rails:chat_5"
  end

  test "dispatch/3 fans out to local PubSub subscribers", %{instance: instance} do
    Phoenix.PubSub.subscribe(Embassy.Rails.BroadcastTest.PubSub, Broadcast.topic("chat_5"))

    Broadcast.dispatch(instance, "chat_5", %{data: "hello", exclude_socket: nil})

    assert_receive {:cable_broadcast, "chat_5", %{data: "hello"}}, 100
  end

  test "dispatch/3 does not deliver to subscribers of a different stream", %{instance: instance} do
    Phoenix.PubSub.subscribe(Embassy.Rails.BroadcastTest.PubSub, Broadcast.topic("other"))

    Broadcast.dispatch(instance, "chat_5", %{data: "hello"})

    refute_receive {:cable_broadcast, _, _}, 50
  end
end
