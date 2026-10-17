defmodule Embassy.Rails.BroadcastClusterTest do
  use ExUnit.Case, async: false

  alias Embassy.Rails.Broadcast

  test "scope/1" do
    assert Broadcast.scope(:redis) == :local
    assert Broadcast.scope(:http) == :cluster
    assert Broadcast.scope(:redisx) == :cluster
  end

  @tag :distributed
  test "dispatch/3 on an :http instance reaches subscribers on another node" do
    unless Node.alive?(), do: {:ok, _} = Node.start(:"primary@127.0.0.1", :longnames)

    {:ok, peer, node} =
      :peer.start_link(%{
        name: :"peer#{System.unique_integer([:positive])}",
        host: ~c"127.0.0.1",
        longnames: true,
        args: [~c"-setcookie", Atom.to_charlist(Node.get_cookie())]
      })

    # The peer is linked to the test process, so it also goes down if the
    # test fails before the explicit stop below.
    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:phoenix_pubsub])

    pubsub = __MODULE__.PubSub
    start_supervised!({Phoenix.PubSub, name: pubsub})
    :ok = :erpc.call(node, Embassy.Rails.Test.RemoteSubscriber, :start_pubsub, [pubsub])

    test_pid = self()

    :erpc.call(node, Embassy.Rails.Test.RemoteSubscriber, :start, [
      pubsub,
      Broadcast.topic("s1"),
      test_pid
    ])

    assert_receive :subscribed, 2_000
    # Let the PG2 adapter see the remote subscriber's group membership.
    Process.sleep(200)

    start_supervised!({Finch, name: __MODULE__.Finch})
    start_supervised!({Embassy, name: __MODULE__.Embassy, finch: __MODULE__.Finch})

    start_supervised!(
      {Embassy.Rails,
       name: __MODULE__.Instance,
       embassy: __MODULE__.Embassy,
       pubsub: pubsub,
       rpc: [transport: {:http, "http://example.test"}, secret: "s"],
       broadcast: [source: :http, key: "k"]}
    )

    Broadcast.dispatch(__MODULE__.Instance, "s1", %{data: "x", exclude_socket: nil})
    assert_receive {:remote_got, {:cable_broadcast, "s1", %{data: "x"}}}, 2_000
    :peer.stop(peer)
  end
end
