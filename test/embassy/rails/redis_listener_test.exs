defmodule Embassy.Rails.RedisListenerTest do
  use ExUnit.Case, async: true

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
end
