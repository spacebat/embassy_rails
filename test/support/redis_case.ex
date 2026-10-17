defmodule Embassy.Rails.Test.RedisCase do
  @moduledoc """
  Case template for tests that need a real Redis.

  Tests using it are tagged `:redis` and run only when `REDIS_URL` is
  set (see `test/test_helper.exs`). Each test gets a supervised `Redix`
  connection and its own stream key, deleted when the test exits.

      docker run --rm -d --name embassy-redis -p 6379:6379 redis:7
      REDIS_URL=redis://localhost:6379/15 mix test
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      @moduletag :redis

      import Embassy.Rails.Test.RedisCase
    end
  end

  setup do
    url = System.fetch_env!("REDIS_URL")
    conn = start_supervised!({Redix, url}, id: :redis_case_conn)
    stream = "test:#{System.unique_integer([:positive])}"

    on_exit(fn ->
      # The supervised connection is gone by now; clean up with a fresh one.
      {:ok, cleanup} = Redix.start_link(url)
      Redix.command!(cleanup, ["DEL", stream])
      Redix.stop(cleanup)
    end)

    %{redis: conn, redis_url: url, stream: stream}
  end

  @doc "Append an entry the way AnyCable's Ruby redisx adapter does; returns its id."
  @spec xadd_payload(Redix.connection(), String.t(), binary()) :: String.t()
  def xadd_payload(conn, stream, payload) do
    Redix.command!(conn, ["XADD", stream, "*", "payload", payload])
  end
end
