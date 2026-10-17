defmodule Mix.Tasks.EmbassyRails.InstallTest do
  use ExUnit.Case, async: true

  import Igniter.Test

  alias Mix.Tasks.EmbassyRails.Install

  # Shared install args for a phx_test_project (app prefix `Test`).
  defp base_args(extra) do
    [
      "--yes",
      "--name",
      "Test.Embassy.Rails",
      "--embassy",
      "Test.Embassy",
      "--pubsub",
      "Test.PubSub",
      "--finch",
      "Test.Finch",
      "--transport",
      "uds",
      "--address",
      "/tmp/puma.sock",
      "--secret",
      "topsecret"
    ] ++ extra
  end

  defp install(extra) do
    phx_test_project()
    |> Igniter.compose_task("embassy_rails.install", base_args(extra))
  end

  # Post-run content of a file, for substring assertions.
  defp rendered(igniter, path) do
    igniter
    |> apply_igniter!()
    |> Map.fetch!(:rewrite)
    |> Rewrite.source!(path)
    |> Rewrite.Source.get(:content)
  end

  describe "info/2" do
    test "declares the option schema and composes embassy.install" do
      info = Install.info([], nil)

      assert info.schema[:name] == :string
      assert info.schema[:embassy] == :string
      assert info.schema[:pubsub] == :string
      assert info.schema[:finch] == :string
      assert info.schema[:transport] == :string
      assert info.schema[:address] == :string
      assert info.schema[:secret] == :string
      assert info.schema[:source] == :string
      assert info.schema[:key] == :string
      assert info.schema[:redis_url] == :string
      assert info.schema[:channel] == :string
      assert "embassy.install" in info.composes
    end
  end

  describe "base composition" do
    test "runs on a phoenix project without raising" do
      install([]) |> apply_igniter!()
    end

    test "composes the embassy base (Finch + Embassy) into the tree" do
      content = rendered(install([]), "lib/test/application.ex")
      assert content =~ "Finch"
      assert content =~ "{Embassy, [name: Test.Embassy, finch: Test.Finch]}"
    end
  end

  describe "supervision tree" do
    test "adds Phoenix.PubSub and the Embassy.Rails child with rpc + http broadcast" do
      content = rendered(install([]), "lib/test/application.ex")

      assert content =~ "Phoenix.PubSub"
      assert content =~ "Embassy.Rails"
      assert content =~ "name: Test.Embassy.Rails"
      assert content =~ "embassy: Test.Embassy"
      assert content =~ "pubsub: Test.PubSub"
      assert content =~ "transport: {:uds, \"/tmp/puma.sock\"}"
      assert content =~ "secret: \"topsecret\""
      assert content =~ "source: :http"
    end

    test "Embassy is positioned before Embassy.Rails in the children list" do
      content = rendered(install([]), "lib/test/application.ex")
      embassy_at = :binary.match(content, "{Embassy,") |> elem(0)
      rails_at = :binary.match(content, "{Embassy.Rails,") |> elem(0)
      assert embassy_at < rails_at
    end

    test "is idempotent for the supervision tree on a second run" do
      twice =
        install([])
        |> apply_igniter!()
        |> Igniter.compose_task("embassy_rails.install", base_args([]))

      assert_unchanged(twice, "lib/test/application.ex")
    end
  end

  describe "router scope (http source)" do
    test "adds /cable, /_broadcast, and / forwards in match order" do
      content = rendered(install([]), "lib/test_web/router.ex")

      assert content =~ "Embassy.Rails.Upgrade"
      assert content =~ "embassy_rails: Test.Embassy.Rails"
      assert content =~ "subprotocols: [\"actioncable-v1-json\"]"
      assert content =~ "Embassy.Rails.BroadcastPlug"
      assert content =~ "Embassy.ReverseProxy"

      cable_at = :binary.match(content, "/cable") |> elem(0)
      broadcast_at = :binary.match(content, "/_broadcast") |> elem(0)
      proxy_at = :binary.match(content, "Embassy.ReverseProxy") |> elem(0)
      assert cable_at < broadcast_at
      assert broadcast_at < proxy_at
    end

    test "the /cable forward carries no handler option" do
      content = rendered(install([]), "lib/test_web/router.ex")
      refute content =~ "handler:"
    end
  end

  describe "redis source" do
    defp install_redis(extra) do
      phx_test_project()
      |> Igniter.compose_task(
        "embassy_rails.install",
        base_args(["--source", "redis", "--redis-url", "redis://localhost:6379/1"]) ++ extra
      )
    end

    test "writes the redis broadcast config and adds the redix dep" do
      igniter = install_redis([])
      app = rendered(igniter, "lib/test/application.ex")
      mix = rendered(igniter, "mix.exs")

      assert app =~ "source: :redis"
      assert app =~ "redis: \"redis://localhost:6379/1\""
      assert mix =~ ":redix"
    end

    test "does not add a /_broadcast route for the redis source" do
      content = rendered(install_redis([]), "lib/test_web/router.ex")
      refute content =~ "Embassy.Rails.BroadcastPlug"
      assert content =~ "Embassy.Rails.Upgrade"
    end

    defp install_redisx(extra) do
      phx_test_project()
      |> Igniter.compose_task(
        "embassy_rails.install",
        base_args(["--source", "redisx", "--redis-url", "redis://localhost:6379/1"]) ++ extra
      )
    end

    test "writes the redisx broadcast config and adds the redix dep" do
      igniter = install_redisx([])
      app = rendered(igniter, "lib/test/application.ex")
      mix = rendered(igniter, "mix.exs")

      assert app =~ "source: :redisx"
      assert app =~ "redis: \"redis://localhost:6379/1\""
      assert mix =~ ~s({:redix, "~> 1.0"})
    end

    test "--stream is written for redisx" do
      app = rendered(install_redisx(["--stream", "custom"]), "lib/test/application.ex")
      assert app =~ "stream: \"custom\""
    end

    test "does not add a /_broadcast route for the redisx source" do
      content = rendered(install_redisx([]), "lib/test_web/router.ex")
      refute content =~ "Embassy.Rails.BroadcastPlug"
      assert content =~ "Embassy.Rails.Upgrade"
    end

    test "redisx source emits a notice naming the Rails adapter and clustered PubSub" do
      assert_has_notice(
        install_redisx([]),
        &(&1 =~ "broadcast_adapter: redisx" and &1 =~ "Phoenix.PubSub")
      )
    end

    test "http source does not add the redix dep" do
      mix = rendered(install([]), "mix.exs")
      refute mix =~ ":redix"
    end
  end

  describe "notices" do
    test "warns when --yes leaves the secret as a placeholder" do
      igniter =
        phx_test_project()
        |> Igniter.compose_task("embassy_rails.install", [
          "--yes",
          "--name",
          "Test.Embassy.Rails",
          "--embassy",
          "Test.Embassy",
          "--pubsub",
          "Test.PubSub",
          "--finch",
          "Test.Finch",
          "--transport",
          "uds",
          "--address",
          "/tmp/puma.sock"
        ])

      assert_has_notice(igniter, &(&1 =~ ~r/RPC secret/))
    end

    test "redis source emits a redix / deps.get notice" do
      assert_has_notice(install_redis([]), &(&1 =~ ~r/redix/))
    end
  end
end
