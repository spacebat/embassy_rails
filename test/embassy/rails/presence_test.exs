defmodule Embassy.Rails.PresenceTest do
  use ExUnit.Case, async: true

  alias Embassy.Rails.{Broadcast, Presence}

  setup do
    start_supervised!({Finch, name: __MODULE__.Finch})
    start_supervised!({Phoenix.PubSub, name: __MODULE__.PubSub})
    start_supervised!({Embassy, name: __MODULE__.Embassy, finch: __MODULE__.Finch})
    {:ok, instance: start_instance(__MODULE__.Instance, presence: [ttl_ms: 50])}
  end

  defp start_instance(name, extra) do
    start_supervised!(
      {Embassy.Rails,
       [
         name: name,
         embassy: __MODULE__.Embassy,
         pubsub: __MODULE__.PubSub,
         rpc: [transport: {:http, "http://example.test"}, secret: "shh"],
         broadcast: [source: :http, key: "bcast"]
       ] ++ extra},
      id: name
    )

    name
  end

  defp subscribe(stream), do: Phoenix.PubSub.subscribe(__MODULE__.PubSub, Broadcast.topic(stream))

  # Joins from a separate process that stays alive until told to exit.
  defp spawn_joiner(instance, stream, sid, id) do
    test = self()

    pid =
      spawn(fn ->
        send(test, {:joined, Presence.join(instance, stream, sid, id, nil)})

        receive do
          :exit -> :ok
        end
      end)

    assert_receive {:joined, :ok}
    pid
  end

  test "join and leave", %{instance: instance} do
    subscribe("room")

    assert :ok = Presence.join(instance, "room", "sid1", "u1", %{"name" => "Bart"})

    assert_receive {:cable_presence, "room",
                    %{"type" => "join", "id" => "u1", "info" => %{"name" => "Bart"}}}

    assert :ok = Presence.leave(instance, "room", "sid1")
    assert_receive {:cable_presence, "room", %{"type" => "leave", "id" => "u1"}}, 20
  end

  test "leave without a join is a no-op", %{instance: instance} do
    assert :ok = Presence.leave(instance, "room", "nobody")
  end

  test "id mismatch", %{instance: instance} do
    assert :ok = Presence.join(instance, "room", "sid1", "u1", nil)
    assert {:error, :id_mismatch} = Presence.join(instance, "room", "sid1", "u2", nil)
  end

  test "info", %{instance: instance} do
    :ok = Presence.join(instance, "room", "sid1", "u1", %{"n" => 1})
    :ok = Presence.join(instance, "room", "sid2", "u2", %{"n" => 2})

    assert {:ok, %{"type" => "info", "total" => 2, "records" => records}} =
             Presence.info(instance, "room", true)

    assert Enum.sort_by(records, & &1["id"]) == [
             %{"id" => "u1", "info" => %{"n" => 1}},
             %{"id" => "u2", "info" => %{"n" => 2}}
           ]

    assert {:ok, info} = Presence.info(instance, "room", false)
    assert info == %{"type" => "info", "total" => 2}

    assert {:ok, %{"type" => "info", "total" => 0}} == Presence.info(instance, "empty", true)
  end

  test "info counts one record per presence id", %{instance: instance} do
    :ok = Presence.join(instance, "room", "sid1", "u1", %{"tab" => 1})
    :ok = Presence.join(instance, "room", "sid2", "u1", %{"tab" => 2})

    assert {:ok, %{"total" => 1, "records" => [%{"id" => "u1", "info" => %{"tab" => 2}}]}} =
             Presence.info(instance, "room", true)
  end

  test "a record without info has no info key", %{instance: instance} do
    :ok = Presence.join(instance, "room", "sid1", "u1", nil)

    assert {:ok, %{"records" => [%{"id" => "u1"} = record]}} =
             Presence.info(instance, "room", true)

    refute Map.has_key?(record, "info")
  end

  test "linger", %{instance: instance} do
    subscribe("room")
    pid = spawn_joiner(instance, "room", "sid1", "u1")
    assert_receive {:cable_presence, "room", %{"type" => "join"}}

    send(pid, :exit)
    refute_receive {:cable_presence, "room", %{"type" => "leave"}}, 30
    assert_receive {:cable_presence, "room", %{"type" => "leave", "id" => "u1"}}, 200
  end

  test "rejoin within ttl", %{instance: instance} do
    subscribe("room")
    old = spawn_joiner(instance, "room", "sid1", "u1")
    assert_receive {:cable_presence, "room", %{"type" => "join"}}

    send(old, :exit)
    _new = spawn_joiner(instance, "room", "sid2", "u1")

    refute_receive {:cable_presence, "room", _}, 200
  end

  test "server crash", %{instance: instance} do
    subscribe("room")
    :ok = Presence.join(instance, "room", "sid1", "u1", nil)
    :ok = Presence.join(instance, "room", "sid2", "u2", nil)
    assert_receive {:cable_presence, "room", %{"type" => "join"}}
    assert_receive {:cable_presence, "room", %{"type" => "join"}}

    instance |> Presence.server_name() |> Process.whereis() |> Process.exit(:kill)

    assert_receive {:cable_presence, "room", %{"type" => "leave", "id" => id1}}, 500
    assert_receive {:cable_presence, "room", %{"type" => "leave", "id" => id2}}, 500
    assert Enum.sort([id1, id2]) == ["u1", "u2"]

    wait_for(fn -> Process.whereis(Presence.server_name(instance)) end)
    assert :ok = Presence.join(instance, "room", "sid3", "u3", nil)
    assert_receive {:cable_presence, "room", %{"type" => "join", "id" => "u3"}}
  end

  test "a tracker shard crash sends a leave for what it had announced", %{instance: instance} do
    subscribe("room")
    :ok = Presence.join(instance, "room", "sid1", "u1", nil)
    assert_receive {:cable_presence, "room", %{"type" => "join", "id" => "u1"}}

    instance
    |> Presence.tracker_name()
    |> Phoenix.Tracker.Shard.name_for_number(0)
    |> Process.whereis()
    |> Process.exit(:kill)

    assert_receive {:cable_presence, "room", %{"type" => "leave", "id" => "u1"}}, 1_000
  end

  test "remote nodes keep a dead node's entries for ttl_ms (the tracker down_period)" do
    instance = start_instance(__MODULE__.LongTtl, presence: [ttl_ms: 90_000])

    shard =
      instance
      |> Presence.tracker_name()
      |> Phoenix.Tracker.Shard.name_for_number(0)
      |> Process.whereis()

    assert %{down_period: 90_000} = :sys.get_state(shard)
  end

  test "a session can rejoin after the tracker shard crashes", %{instance: instance} do
    :ok = Presence.join(instance, "room", "sid1", "u1", nil)
    server = Process.whereis(Presence.server_name(instance))

    instance
    |> Presence.tracker_name()
    |> Phoenix.Tracker.Shard.name_for_number(0)
    |> Process.whereis()
    |> Process.exit(:kill)

    wait_for(fn ->
      pid = Process.whereis(Presence.server_name(instance))
      pid && pid != server
    end)

    assert :ok = Presence.join(instance, "room", "sid1", "u1", nil)
    assert {:ok, %{"total" => 1}} = Presence.info(instance, "room", false)
  end

  test "join and leave do not exit the caller while the server is down", %{instance: instance} do
    :ok = Supervisor.terminate_child(Embassy.Rails.services_name(instance), Presence)

    assert {:error, :unavailable} = Presence.join(instance, "room", "sid1", "u1", nil)
    assert :ok = Presence.leave(instance, "room", "sid1")
  end

  defp wait_for(fun, tries \\ 50) do
    cond do
      fun.() -> :ok
      tries == 0 -> flunk("condition never became true")
      true -> Process.sleep(10) && wait_for(fun, tries - 1)
    end
  end

  test "disabled" do
    instance = start_instance(__MODULE__.Disabled, presence: false)

    assert {:error, :disabled} = Presence.join(instance, "room", "sid1", "u1", nil)
    assert {:error, :disabled} = Presence.info(instance, "room", true)
    assert Process.whereis(Presence.tracker_name(instance)) == nil
  end

  test "defaults to a 60s ttl with no api secret or upstream" do
    instance = start_instance(__MODULE__.Defaults, [])

    assert Embassy.Rails.Config.snapshot(instance).presence ==
             %{ttl_ms: 60_000, api_secret: nil, upstream: nil}
  end

  test "keeps api_secret and upstream" do
    instance =
      start_instance(__MODULE__.Full,
        presence: [ttl_ms: 10, api_secret: "s", upstream: "http://ac.example"]
      )

    assert Embassy.Rails.Config.snapshot(instance).presence ==
             %{ttl_ms: 10, api_secret: "s", upstream: "http://ac.example"}
  end

  test "rejects unknown presence keys" do
    assert_raise ArgumentError, ~r/:ttl/, fn ->
      Embassy.Rails.normalize_presence!(ttl: 50)
    end
  end

  test "invalid option" do
    assert_raise ArgumentError, ~r/:presence/, fn ->
      Embassy.Rails.start_link(
        name: __MODULE__.Invalid,
        embassy: __MODULE__.Embassy,
        pubsub: __MODULE__.PubSub,
        rpc: [transport: {:http, "http://example.test"}, secret: "shh"],
        broadcast: [source: :http, key: "bcast"],
        presence: [ttl_ms: 0]
      )
    end
  end
end
