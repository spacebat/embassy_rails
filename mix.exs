defmodule EmbassyRails.MixProject do
  use Mix.Project

  @source_url "https://github.com/spacebat/embassy_rails"
  @version "0.4.1"

  def project do
    [
      app: :embassy_rails,
      version: @version,
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      hex: [
        # cowlib is only pulled in by bypass (test-only) and has no fixed
        # release yet; library consumers resolve their own cowlib.
        ignore_advisories: ["EEF-CVE-2026-43966"]
      ],
      description: description(),
      package: package(),
      docs: docs(),
      source_url: @source_url
    ]
  end

  def cli do
    [preferred_envs: [ci: :test]]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # `mix ci` is what GitHub Actions runs. Dialyzer is deliberately not in it;
  # run `mix dialyzer` by hand.
  defp aliases do
    [
      ci: [
        "deps.unlock --check-unused",
        "hex.audit",
        "format --check-formatted",
        "compile --warnings-as-errors --force",
        "credo",
        "sobelow --skip --exit",
        "test --warnings-as-errors"
      ]
    ]
  end

  defp deps do
    [
      embassy_dep(),
      {:igniter, "~> 0.6", optional: true},
      {:plug, "~> 1.16"},
      {:websock_adapter, "~> 0.5"},
      {:phoenix_pubsub, "~> 2.1"},
      {:jason, "~> 1.4"},
      {:redix, "~> 1.0", optional: true},
      {:grpc, "~> 1.0", optional: true},
      {:protobuf, "~> 0.17", optional: true},
      {:grpc_server, "~> 1.0", only: :test},
      {:telemetry, "~> 1.2"},
      {:telemetry_metrics, "~> 1.0", optional: true},
      {:bypass, "~> 2.1", only: :test},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.13", only: [:dev, :test], runtime: false},
      {:ex_slop, "~> 0.4", only: [:dev, :test], runtime: false}
    ]
  end

  # embassy is a sibling package in the embassy family. By default it
  # resolves from the spacebat local registry as a normal versioned hex
  # dependency, which is also what gets baked into the released package
  # metadata. For local cross-package development set EMBASSY_PATH=1 to use
  # a path dependency instead, so edits to ../embassy flow in without a
  # republish. See ~/.claude/skills/using-spacebat-hex-registry.
  defp embassy_dep do
    if System.get_env("EMBASSY_PATH") do
      {:embassy, path: "../embassy"}
    else
      {:embassy, "~> 0.4", repo: "spacebat"}
    end
  end

  defp description do
    "Rails / ActionCable / AnyCable adapter for the embassy hex package. " <>
      "Terminates ActionCable WebSockets in Phoenix and bridges to Rails via AnyCable HTTP RPC."
  end

  defp package do
    [
      maintainers: ["Andrew Kirkpatrick"],
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib priv mix.exs README.md CHANGELOG.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      source_ref: "v#{@version}",
      extras: ["README.md", "CHANGELOG.md"]
    ]
  end
end
