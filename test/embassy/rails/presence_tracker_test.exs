defmodule Embassy.Rails.PresenceTrackerTest do
  use ExUnit.Case, async: true

  alias Embassy.Rails.{Broadcast, PresenceTracker}

  setup do
    start_supervised!({Phoenix.PubSub, name: __MODULE__.PubSub})
    Phoenix.PubSub.subscribe(__MODULE__.PubSub, Broadcast.topic("s"))
    {:ok, state} = PresenceTracker.init(instance: :inst, pubsub: __MODULE__.PubSub)
    {:ok, state: state}
  end

  defp join(sid, id, info \\ nil), do: {sid, %{id: id, info: info}}

  defp diff(state, joins, leaves) do
    {:ok, state} = PresenceTracker.handle_diff(%{"s" => {joins, leaves}}, state)
    state
  end

  test "first join broadcasts", %{state: state} do
    diff(state, [join("sid1", "u1", %{"n" => 1})], [])

    assert_receive {:cable_presence, "s",
                    %{"type" => "join", "id" => "u1", "info" => %{"n" => 1}}}
  end

  test "two sessions with one id", %{state: state} do
    state = diff(state, [join("sid1", "u1")], [])
    assert_receive {:cable_presence, "s", %{"type" => "join", "id" => "u1"}}

    state = diff(state, [join("sid2", "u1")], [])
    state = diff(state, [], [join("sid1", "u1")])
    refute_receive {:cable_presence, _, _}, 20

    diff(state, [], [join("sid2", "u1")])
    assert_receive {:cable_presence, "s", %{"type" => "leave", "id" => "u1"} = leave}
    refute Map.has_key?(leave, "info")
    refute_receive {:cable_presence, _, _}, 20
  end

  test "a meta update in one diff sends nothing", %{state: state} do
    state = diff(state, [join("sid1", "u1", %{"v" => 1})], [])
    assert_receive {:cable_presence, "s", %{"type" => "join"}}

    diff(state, [join("sid1", "u1", %{"v" => 2})], [join("sid1", "u1", %{"v" => 1})])
    refute_receive {:cable_presence, _, _}, 20
  end

  test "a join from a remote node's session counts the same", %{state: state} do
    diff(state, [join("remote-node-sid", "u9", %{"a" => 1})], [])

    assert_receive {:cable_presence, "s",
                    %{"type" => "join", "id" => "u9", "info" => %{"a" => 1}}}
  end

  test "a join with nil info has no info key", %{state: state} do
    diff(state, [join("sid1", "u1")], [])
    assert_receive {:cable_presence, "s", %{"type" => "join"} = event}
    refute Map.has_key?(event, "info")
  end
end
