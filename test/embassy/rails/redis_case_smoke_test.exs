defmodule Embassy.Rails.RedisCaseSmokeTest do
  use Embassy.Rails.Test.RedisCase

  test "xadd_payload appends one entry to the test stream", %{redis: redis, stream: stream} do
    assert is_binary(xadd_payload(redis, stream, ~s({"stream":"s","data":"x"})))
    assert Redix.command!(redis, ["XLEN", stream]) == 1
  end
end
