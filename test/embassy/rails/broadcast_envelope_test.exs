defmodule Embassy.Rails.BroadcastEnvelopeTest do
  use ExUnit.Case, async: true

  alias Embassy.Rails.BroadcastEnvelope

  describe "decode/1" do
    test "decodes a single envelope" do
      assert BroadcastEnvelope.decode(~s({"stream":"chat:1","data":"hi"})) ==
               {:ok, [{:broadcast, "chat:1", %{data: "hi", exclude_socket: nil}}]}
    end

    test "extracts meta.exclude_socket" do
      payload = ~s({"stream":"chat:1","data":"hi","meta":{"exclude_socket":"sock-9"}})

      assert BroadcastEnvelope.decode(payload) ==
               {:ok, [{:broadcast, "chat:1", %{data: "hi", exclude_socket: "sock-9"}}]}
    end

    test "decodes a batch in order" do
      payload = ~s([{"stream":"a","data":"1"},{"stream":"b","data":"2"}])

      assert BroadcastEnvelope.decode(payload) ==
               {:ok,
                [
                  {:broadcast, "a", %{data: "1", exclude_socket: nil}},
                  {:broadcast, "b", %{data: "2", exclude_socket: nil}}
                ]}
    end

    test "skips invalid elements of a batch" do
      payload = ~s([{"stream":"a","data":"1"},{"data":"x"},42])

      assert BroadcastEnvelope.decode(payload) ==
               {:ok, [{:broadcast, "a", %{data: "1", exclude_socket: nil}}]}
    end

    test "decodes a command" do
      payload = ~s({"command":"disconnect","payload":{"identifier":"u1","reconnect":true}})

      assert BroadcastEnvelope.decode(payload) ==
               {:ok, [{:command, "disconnect", %{"identifier" => "u1", "reconnect" => true}}]}
    end

    test "returns :invalid_json for malformed JSON" do
      assert BroadcastEnvelope.decode("not json") == {:error, :invalid_json}
    end

    test "returns :not_an_envelope for valid JSON of the wrong shape" do
      assert BroadcastEnvelope.decode(~s({"data":"hi"})) == {:error, :not_an_envelope}
      assert BroadcastEnvelope.decode(~s({"stream":42})) == {:error, :not_an_envelope}
      assert BroadcastEnvelope.decode("42") == {:error, :not_an_envelope}
    end
  end
end
