defmodule Embassy.RailsTest do
  use ExUnit.Case, async: true

  setup do
    start_supervised!({Finch, name: Embassy.RailsTest.Finch})
    start_supervised!({Phoenix.PubSub, name: Embassy.RailsTest.PubSub})
    start_supervised!({Embassy, name: Embassy.RailsTest.Embassy, finch: Embassy.RailsTest.Finch})
    :ok
  end

  defp valid_opts(name) do
    [
      name: name,
      embassy: Embassy.RailsTest.Embassy,
      pubsub: Embassy.RailsTest.PubSub,
      rpc: [transport: {:http, "http://example.test"}, secret: "shh"],
      broadcast: [source: :http, key: "bcast-key"]
    ]
  end

  test "starts cleanly with all required opts" do
    {:ok, pid} = start_supervised({Embassy.Rails, valid_opts(Embassy.RailsTest.Instance)})
    assert Process.alive?(pid)
  end

  test "raises ArgumentError when :name is missing" do
    opts = valid_opts(:ignored) |> Keyword.delete(:name)

    assert_raise ArgumentError, ~r/required option :name/, fn ->
      Embassy.Rails.start_link(opts)
    end
  end

  test "raises ArgumentError when :embassy is missing" do
    opts = valid_opts(Embassy.RailsTest.NoEmbassy) |> Keyword.delete(:embassy)

    assert_raise ArgumentError, ~r/required option :embassy/, fn ->
      Embassy.Rails.start_link(opts)
    end
  end

  test "raises ArgumentError when :pubsub is missing" do
    opts = valid_opts(Embassy.RailsTest.NoPubSub) |> Keyword.delete(:pubsub)

    assert_raise ArgumentError, ~r/required option :pubsub/, fn ->
      Embassy.Rails.start_link(opts)
    end
  end

  test "raises ArgumentError when :rpc is missing" do
    opts = valid_opts(Embassy.RailsTest.NoRpc) |> Keyword.delete(:rpc)

    assert_raise ArgumentError, ~r/required option :rpc/, fn ->
      Embassy.Rails.start_link(opts)
    end
  end

  test "raises ArgumentError when :broadcast is missing" do
    opts = valid_opts(Embassy.RailsTest.NoBcast) |> Keyword.delete(:broadcast)

    assert_raise ArgumentError, ~r/required option :broadcast/, fn ->
      Embassy.Rails.start_link(opts)
    end
  end

  test "raises Embassy.NotStartedError when :embassy parent isn't running" do
    opts =
      valid_opts(Embassy.RailsTest.OrphanedRails)
      |> Keyword.put(:embassy, Definitely.Not.Started)

    assert_raise Embassy.NotStartedError, ~r/Definitely.Not.Started/, fn ->
      Embassy.Rails.start_link(opts)
    end
  end

  test "multiple named instances coexist" do
    {:ok, _a} = start_supervised({Embassy.Rails, valid_opts(Embassy.RailsTest.A)}, id: :rails_a)
    {:ok, _b} = start_supervised({Embassy.Rails, valid_opts(Embassy.RailsTest.B)}, id: :rails_b)

    assert Process.whereis(Embassy.RailsTest.A)
    assert Process.whereis(Embassy.RailsTest.B)
  end
end
