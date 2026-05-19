defmodule Embassy.Rails do
  @moduledoc """
  Per-instance supervisor and public entry point for the embassy_rails
  ActionCable / AnyCable adapter.

  Consumers add `{Embassy.Rails, name: ..., embassy: ..., pubsub: ...,
  rpc: ..., broadcast: ...}` to their application supervision tree
  alongside `{Embassy, name: ..., finch: ...}`. Plugs and the WS
  handler look up configuration by the given `:name` via
  `Embassy.Rails.Config.snapshot/1`.

  ## Required options

    * `:name` — atom; the instance name. Must be unique within the BEAM.
    * `:embassy` — atom; the name of a running `Embassy` instance whose
      Finch this adapter will use.
    * `:pubsub` — atom; the name of a running `Phoenix.PubSub`
      instance the consumer owns.
    * `:rpc` — keyword. Must include `:transport` (`{:uds, path}` or
      `{:http, base_url}`) and `:secret` (bearer token).
    * `:broadcast` — keyword. Must include `:source` (`:http` in v0.1)
      and `:key` (bearer token for `POST /_broadcast`).

  ## Example

      children = [
        {Finch, name: MyApp.Finch},
        {Phoenix.PubSub, name: MyApp.PubSub},
        {Embassy, name: MyApp.Embassy, finch: MyApp.Finch},
        {Embassy.Rails,
           name: MyApp.Embassy.Rails,
           embassy: MyApp.Embassy,
           pubsub: MyApp.PubSub,
           rpc: [transport: {:uds, "/tmp/puma.sock"}, secret: "..."],
           broadcast: [source: :http, key: "..."]}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)
  """

  use Supervisor

  @doc """
  Start an Embassy.Rails instance under a supervisor.

  See the module doc for required options.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    name = fetch_required!(opts, :name)
    embassy = fetch_required!(opts, :embassy)
    _pubsub = fetch_required!(opts, :pubsub)
    _rpc = fetch_required!(opts, :rpc)
    _broadcast = fetch_required!(opts, :broadcast)
    # Validate the parent Embassy instance is running in the caller's
    # process so that Embassy.NotStartedError is raised synchronously
    # (making assert_raise work in tests, and giving a clean error in
    # production rather than a supervisor crash log).
    Embassy.Config.snapshot(embassy)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Child-spec for use in a consumer's supervision tree.

  The `:id` is derived from `:name` so multiple Embassy.Rails
  instances cohabit in the same tree without collision.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    name = fetch_required!(opts, :name)

    %{
      id: name,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc false
  def registry_name(name), do: Module.concat(name, Registry)

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    embassy = Keyword.fetch!(opts, :embassy)
    pubsub = Keyword.fetch!(opts, :pubsub)
    rpc = Keyword.fetch!(opts, :rpc)
    broadcast = Keyword.fetch!(opts, :broadcast)

    # Raises Embassy.NotStartedError if the parent isn't up first.
    parent = Embassy.Config.snapshot(embassy)

    snapshot = %{
      name: name,
      embassy: embassy,
      finch: parent.finch,
      pubsub: pubsub,
      rpc: rpc,
      broadcast: broadcast
    }

    children = [
      {Registry, keys: :unique, name: registry_name(name), meta: [config: snapshot]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp fetch_required!(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} -> value
      :error -> raise ArgumentError, "required option #{inspect(key)} is missing"
    end
  end
end
