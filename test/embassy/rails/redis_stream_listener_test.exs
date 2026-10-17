defmodule Embassy.Rails.RedisStreamListenerTest do
  use ExUnit.Case, async: true

  alias Embassy.Rails.RedisStreamListener

  describe "parse_read_reply/1" do
    test "returns no entries on a block timeout" do
      assert RedisStreamListener.parse_read_reply(nil) == []
    end

    test "turns field lists into maps" do
      reply = [["st", [["1-0", ["payload", "x"]], ["2-0", ["payload", "y", "extra", "z"]]]]]

      assert RedisStreamListener.parse_read_reply(reply) == [
               {"1-0", %{"payload" => "x"}},
               {"2-0", %{"payload" => "y", "extra" => "z"}}
             ]
    end
  end

  describe "parse_autoclaim_reply/1" do
    test "drops entries deleted while pending (Redis 7 shape)" do
      reply = ["0-0", [["1-0", ["payload", "x"]], nil], []]
      assert RedisStreamListener.parse_autoclaim_reply(reply) == [{"1-0", %{"payload" => "x"}}]
    end

    test "accepts the two-element Redis 6.2 shape" do
      reply = ["0-0", [["1-0", ["payload", "x"]], nil]]
      assert RedisStreamListener.parse_autoclaim_reply(reply) == [{"1-0", %{"payload" => "x"}}]
    end
  end

  test "the child spec's shutdown outlasts a blocked read" do
    spec =
      Embassy.Rails.RedisStreamListener.child_spec(
        instance: :i,
        redis: "redis://x",
        block_ms: 10_000
      )

    assert spec.shutdown > 10_000

    assert Embassy.Rails.RedisStreamListener.child_spec(instance: :i, redis: "redis://x").shutdown >
             2_000
  end
end
