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

  describe "connection tuning options" do
    for {key, bad} <- [
          ping_interval: 0,
          ping_interval: "3000",
          max_mailbox: 0,
          max_mailbox: :infinity,
          slow_client_ms: -1
        ] do
      test "rejects #{key}: #{inspect(bad)}" do
        opts = valid_opts(Embassy.RailsTest.BadTuning) ++ [{unquote(key), unquote(bad)}]

        assert_raise ArgumentError, ~r/#{unquote(key)}/, fn ->
          Embassy.Rails.start_link(opts)
        end
      end
    end

    test "accepts ping_interval: :infinity and slow_client_ms: 0" do
      opts = valid_opts(Embassy.RailsTest.Tuned) ++ [ping_interval: :infinity, slow_client_ms: 0]
      assert {:ok, _} = start_supervised({Embassy.Rails, opts})
    end
  end

  test "multiple named instances coexist" do
    {:ok, _a} = start_supervised({Embassy.Rails, valid_opts(Embassy.RailsTest.A)}, id: :rails_a)
    {:ok, _b} = start_supervised({Embassy.Rails, valid_opts(Embassy.RailsTest.B)}, id: :rails_b)

    assert Process.whereis(Embassy.RailsTest.A)
    assert Process.whereis(Embassy.RailsTest.B)
  end

  describe "redis broadcast source" do
    setup do
      start_supervised!({Finch, name: __MODULE__.RedisFinch}, id: :redis_finch)
      start_supervised!({Phoenix.PubSub, name: __MODULE__.RedisPubSub}, id: :redis_pubsub)

      start_supervised!({Embassy, name: __MODULE__.RedisEmbassy, finch: __MODULE__.RedisFinch},
        id: :redis_embassy
      )

      :ok
    end

    test "starts a RedisListener child when source is :redis" do
      start_supervised!(
        {Embassy.Rails,
         name: __MODULE__.RedisInstance,
         embassy: __MODULE__.RedisEmbassy,
         pubsub: __MODULE__.RedisPubSub,
         rpc: [transport: {:http, "http://example.test"}, secret: "shh"],
         broadcast: [source: :redis, redis: [host: "127.0.0.1", port: 1]]}
      )

      children = Supervisor.which_children(__MODULE__.RedisInstance)

      assert Enum.any?(children, fn {id, pid, _type, _mods} ->
               id == Embassy.Rails.RedisListener and is_pid(pid)
             end)
    end

    test "does not start a RedisListener child when source is :http" do
      start_supervised!(
        {Embassy.Rails,
         name: __MODULE__.HttpInstance,
         embassy: __MODULE__.RedisEmbassy,
         pubsub: __MODULE__.RedisPubSub,
         rpc: [transport: {:http, "http://example.test"}, secret: "shh"],
         broadcast: [source: :http, key: "bcast"]}
      )

      children = Supervisor.which_children(__MODULE__.HttpInstance)

      refute Enum.any?(children, fn {id, _pid, _type, _mods} ->
               id == Embassy.Rails.RedisListener
             end)
    end
  end

  describe "validate_broadcast!/2" do
    test "raises when source is :redis but redix is unavailable" do
      assert_raise ArgumentError, ~r/requires the :redix dependency/, fn ->
        Embassy.Rails.validate_broadcast!([source: :redis, redis: "redis://x"], false)
      end
    end

    test "passes when source is :redis and redix is available" do
      assert Embassy.Rails.validate_broadcast!([source: :redis, redis: "redis://x"], true) == :ok
    end

    test "passes for the :http source regardless of redix" do
      assert Embassy.Rails.validate_broadcast!([source: :http, key: "k"], false) == :ok
    end

    test "raises when source is :redisx but redix is unavailable" do
      message =
        "Embassy.Rails: broadcast source :redisx requires the :redix dependency; " <>
          "add {:redix, \"~> 1.0\"} to your deps"

      assert_raise ArgumentError, message, fn ->
        Embassy.Rails.validate_broadcast!([source: :redisx, redis: "redis://x"], false)
      end
    end

    test "passes when source is :redisx and redix is available" do
      assert Embassy.Rails.validate_broadcast!([source: :redisx, redis: "redis://x"], true) == :ok
    end

    test "raises when a Redis source has no :redis option" do
      for source <- [:redis, :redisx] do
        assert_raise ArgumentError, ~r/:redis/, fn ->
          Embassy.Rails.validate_broadcast!([source: source], true)
        end
      end
    end
  end
end
