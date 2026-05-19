defmodule Embassy.Rails.ConfigTest do
  use ExUnit.Case, async: true

  test "Embassy.Rails.NotStartedError carries the instance name in its message" do
    err = %Embassy.Rails.NotStartedError{name: MyApp.Embassy.Rails}
    assert Exception.message(err) =~ "MyApp.Embassy.Rails"
    assert Exception.message(err) =~ "not started"
  end

  describe "snapshot/1" do
    setup do
      start_supervised!({Finch, name: Embassy.Rails.ConfigTest.Finch})
      start_supervised!({Phoenix.PubSub, name: Embassy.Rails.ConfigTest.PubSub})

      start_supervised!(
        {Embassy, name: Embassy.Rails.ConfigTest.Embassy, finch: Embassy.Rails.ConfigTest.Finch}
      )

      start_supervised!(
        {Embassy.Rails,
         name: Embassy.Rails.ConfigTest.Instance,
         embassy: Embassy.Rails.ConfigTest.Embassy,
         pubsub: Embassy.Rails.ConfigTest.PubSub,
         rpc: [transport: {:http, "http://example.test"}, secret: "shh"],
         broadcast: [source: :http, key: "bcast"]}
      )

      :ok
    end

    test "returns the snapshot for a running instance" do
      snap = Embassy.Rails.Config.snapshot(Embassy.Rails.ConfigTest.Instance)
      assert snap.name == Embassy.Rails.ConfigTest.Instance
      assert snap.embassy == Embassy.Rails.ConfigTest.Embassy
      assert snap.finch == Embassy.Rails.ConfigTest.Finch
      assert snap.pubsub == Embassy.Rails.ConfigTest.PubSub
      assert snap.rpc[:transport] == {:http, "http://example.test"}
      assert snap.rpc[:secret] == "shh"
      assert snap.broadcast[:source] == :http
      assert snap.broadcast[:key] == "bcast"
    end

    test "raises NotStartedError for an unknown instance name" do
      assert_raise Embassy.Rails.NotStartedError, ~r/Definitely.Not.Started/, fn ->
        Embassy.Rails.Config.snapshot(Definitely.Not.Started)
      end
    end
  end
end
