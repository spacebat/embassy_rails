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
end
