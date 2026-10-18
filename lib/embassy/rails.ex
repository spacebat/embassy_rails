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
    * `:rpc` — keyword. Must include `:transport` (`{:uds, path}`,
      `{:http, base_url}`, or `{:grpc, endpoint}`) and `:secret` (bearer
      token). The `:grpc` form requires the optional `:grpc` and
      `:protobuf` deps. Optional `:timeout` (ms, default 5000) bounds
      each RPC call. Optional `:max_disconnects` (default `1_000`) caps
      the disconnect RPCs in flight at once; one over the cap is dropped
      with a warning, as anycable-go does when its disconnect queue is
      full.
    * `:broadcast` — keyword. Must include `:source`. For
      `source: :http`: `:key` (bearer token for `POST /_broadcast`).
      For `source: :redis` (AnyCable's Pub/Sub broadcaster): `:redis` (a
      `Redix.PubSub` URL string or options keyword) and optionally
      `:channel` (defaults to `"__anycable__"`). For `source: :redisx`
      (AnyCable's Redis Streams broadcaster): `:redis` (a Redis URL or
      `Redix` options) and optionally `:stream` (`"__anycable__"`),
      `:group` (`"bx"`), `:block_ms` (`2000`) and `:consumer`; see
      `Embassy.Rails.RedisStreamListener`. Both Redis sources require the
      optional `:redix` dependency.

  ## Optional options

    * `:ping_interval` — ms between server pings (default `3_000`), or
      `:infinity` to disable them and rely on the proxy or client.
    * `:max_mailbox` — a connection whose mailbox has held more messages
      than this for `:slow_client_ms` when a broadcast arrives is
      disconnected as a slow client (default `1_000`).
    * `:slow_client_ms` — how long a mailbox may stay over `:max_mailbox`
      before the client is disconnected, so a burst that drains is not
      cut off (default `5_000`; `0` disconnects at once).
    * `:headers` — list of extra request header names forwarded to the RPC
      `Env` (case-insensitive), added to the defaults
      `cookie`, `user-agent`, `origin`, `x-forwarded-for`, `x-real-ip`.
    * `:presence` — keyword, or `false` to disable AnyCable presence.
      `:ttl_ms` (default `60_000`) is how long a disconnected client stays
      present; `:api_secret` (default `nil`, no auth) guards
      `Embassy.Rails.PresenceApiPlug`; `:upstream` (default `nil`) is an
      anycable-go base URL whose presence records the API unions in. See
      `Embassy.Rails.Presence`.

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

  ## Redis broadcast source

      {Embassy.Rails,
         name: MyApp.Embassy.Rails,
         embassy: MyApp.Embassy,
         pubsub: MyApp.PubSub,
         rpc: [transport: {:uds, "/tmp/puma.sock"}, secret: "..."],
         broadcast: [source: :redis, redis: "redis://localhost:6379/1"]}

  Requires `{:redix, "~> 1.0"}` in the consumer's deps. For Redis
  Streams, use `source: :redisx` with the same `:redis` option and set
  `broadcast_adapter: redisx` on the Rails side. Each entry reaches one
  node, which re-broadcasts it cluster-wide, so a multi-node deployment
  needs a clustered `Phoenix.PubSub`.
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
    rpc = fetch_required!(opts, :rpc)
    broadcast = fetch_required!(opts, :broadcast)
    validate_broadcast!(broadcast, Code.ensure_loaded?(Redix.PubSub))
    validate_rpc!(rpc, Code.ensure_loaded?(GRPC.Stub))
    normalize_presence!(Keyword.get(opts, :presence, []))
    normalize_tuning!(opts)
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

  @doc false
  def tasks_name(name), do: Module.concat(name, Tasks)

  @doc false
  def services_name(name), do: Module.concat(name, Services)

  @doc false
  def broadcast_source_name(name), do: Module.concat(name, BroadcastSource)

  @doc "Name of the duplicate-key Registry every live Connection registers in (key `:conn`)."
  @spec connections_name(atom()) :: atom()
  def connections_name(name), do: Module.concat(name, Connections)

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    embassy = Keyword.fetch!(opts, :embassy)
    pubsub = Keyword.fetch!(opts, :pubsub)
    rpc = Keyword.fetch!(opts, :rpc)
    broadcast = Keyword.fetch!(opts, :broadcast)

    Embassy.Rails.Drain.reset(name)

    # Raises Embassy.NotStartedError if the parent isn't up first.
    parent = Embassy.Config.snapshot(embassy)

    tuning = normalize_tuning!(opts)

    snapshot = %{
      name: name,
      embassy: embassy,
      finch: parent.finch,
      pubsub: pubsub,
      rpc: rpc,
      broadcast: broadcast,
      presence: normalize_presence!(Keyword.get(opts, :presence, [])),
      headers: forwarded_headers(Keyword.get(opts, :headers, [])),
      tuning: tuning
    }

    services =
      rpc_children(name, rpc) ++
        [
          {Task.Supervisor,
           name: tasks_name(name), max_children: Keyword.get(rpc, :max_disconnects, 1_000)}
        ] ++
        presence_children(name, pubsub, snapshot.presence)

    # rest_for_one: every child after the config Registry reads its meta or
    # registers in it, so a Registry restart restarts them too. The
    # connections Registry comes first, so live connections stay registered
    # (and drainable) through that. Services and the broadcast source are
    # one_for_one groups of their own: a crash in one does not restart its
    # siblings, and a broadcast listener that keeps crashing exhausts only
    # its own group's restart intensity.
    children = [
      {Registry, keys: :duplicate, name: connections_name(name)},
      {Registry,
       keys: :unique, name: registry_name(name), meta: [config: snapshot, tuning: tuning]},
      group(services_name(name), services),
      group(broadcast_source_name(name), broadcast_children(name, broadcast))
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp group(name, children) do
    %{
      id: name,
      type: :supervisor,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one, name: name]]}
    }
  end

  defp fetch_required!(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} -> value
      :error -> raise ArgumentError, "required option #{inspect(key)} is missing"
    end
  end

  @doc false
  def validate_broadcast!(broadcast, redix_available?) do
    case Keyword.get(broadcast, :source) do
      source when source in [:redis, :redisx] ->
        unless redix_available? do
          raise ArgumentError,
                "Embassy.Rails: broadcast source #{inspect(source)} requires the :redix dependency; " <>
                  "add {:redix, \"~> 1.0\"} to your deps"
        end

        unless Keyword.has_key?(broadcast, :redis) do
          raise ArgumentError,
                "Embassy.Rails: broadcast source #{inspect(source)} requires a :redis option " <>
                  "(a Redis URL or Redix options)"
        end

        :ok

      _ ->
        :ok
    end
  end

  @tuning_defaults [ping_interval: 3_000, max_mailbox: 1_000, slow_client_ms: 5_000]

  @doc false
  @spec normalize_tuning!(keyword()) :: %{
          ping_interval: pos_integer() | :infinity,
          max_mailbox: pos_integer(),
          slow_client_ms: non_neg_integer()
        }
  def normalize_tuning!(opts) do
    tuning =
      Map.new(@tuning_defaults, fn {key, default} -> {key, Keyword.get(opts, key, default)} end)

    unless tuning.ping_interval == :infinity or pos_integer?(tuning.ping_interval) do
      raise ArgumentError,
            "Embassy.Rails: :ping_interval must be a positive integer or :infinity, " <>
              "got #{inspect(tuning.ping_interval)}"
    end

    unless pos_integer?(tuning.max_mailbox) do
      raise ArgumentError,
            "Embassy.Rails: :max_mailbox must be a positive integer, got #{inspect(tuning.max_mailbox)}"
    end

    unless is_integer(tuning.slow_client_ms) and tuning.slow_client_ms >= 0 do
      raise ArgumentError,
            "Embassy.Rails: :slow_client_ms must be a non-negative integer, " <>
              "got #{inspect(tuning.slow_client_ms)}"
    end

    tuning
  end

  defp pos_integer?(value), do: is_integer(value) and value > 0

  @default_headers ~w(cookie user-agent origin x-forwarded-for x-real-ip)

  @doc false
  def default_headers, do: @default_headers

  @presence_defaults [ttl_ms: 60_000, api_secret: nil, upstream: nil]

  @doc false
  @spec normalize_presence!(keyword() | false) ::
          %{ttl_ms: pos_integer(), api_secret: binary() | nil, upstream: binary() | nil} | nil
  def normalize_presence!(false), do: nil

  def normalize_presence!(opts) when is_list(opts) do
    case Keyword.keys(opts) -- Keyword.keys(@presence_defaults) do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "Embassy.Rails: unknown :presence option(s) #{inspect(unknown)}; " <>
                "expected #{inspect(Keyword.keys(@presence_defaults))}"
    end

    presence = Map.new(Keyword.merge(@presence_defaults, opts))

    unless is_integer(presence.ttl_ms) and presence.ttl_ms > 0 do
      raise ArgumentError,
            "Embassy.Rails: :presence :ttl_ms must be a positive integer, got #{inspect(presence.ttl_ms)}"
    end

    for key <- [:api_secret, :upstream],
        not (is_nil(presence[key]) or is_binary(presence[key])) do
      raise ArgumentError,
            "Embassy.Rails: :presence #{inspect(key)} must be a string or nil, " <>
              "got #{inspect(presence[key])}"
    end

    presence
  end

  def normalize_presence!(other) do
    raise ArgumentError,
          "Embassy.Rails: :presence must be a keyword list or false, got #{inspect(other)}"
  end

  defp forwarded_headers(extra) do
    extra
    |> Enum.map(&String.downcase/1)
    |> Kernel.++(@default_headers)
    |> Enum.uniq()
  end

  defp presence_children(_name, _pubsub, nil), do: []

  defp presence_children(name, pubsub, presence) do
    # Runs in this supervisor's init, which makes it the table's owner.
    Embassy.Rails.PresenceTracker.new_counts_table(name)

    [
      {Embassy.Rails.PresenceTracker,
       instance: name,
       pubsub: pubsub,
       ttl_ms: presence.ttl_ms,
       name: Embassy.Rails.Presence.tracker_name(name)},
      {Embassy.Rails.Presence, instance: name, ttl_ms: presence.ttl_ms}
    ]
  end

  @doc false
  def validate_rpc!(rpc, grpc_available?) do
    case Keyword.get(rpc, :transport) do
      {:grpc, _endpoint} ->
        unless grpc_available? do
          raise ArgumentError,
                "Embassy.Rails: rpc transport :grpc requires the :grpc and :protobuf " <>
                  "dependencies; add {:grpc, \"~> 1.0\"} and {:protobuf, \"~> 0.17\"} to your deps"
        end

        if is_nil(Keyword.get(rpc, :secret)) do
          raise ArgumentError,
                "Embassy.Rails: rpc transport :grpc requires a :secret (bearer token)"
        end

        :ok

      _ ->
        :ok
    end
  end

  @doc false
  def rpc_children(name, rpc) do
    case Keyword.get(rpc, :transport) do
      {:grpc, endpoint} ->
        [
          {Embassy.Rails.GrpcChannel,
           instance: name, endpoint: endpoint, secret: Keyword.fetch!(rpc, :secret)}
        ]

      _ ->
        []
    end
  end

  defp broadcast_children(name, broadcast) do
    case Keyword.get(broadcast, :source) do
      :redis ->
        [
          {Embassy.Rails.RedisListener,
           instance: name,
           redis: Keyword.fetch!(broadcast, :redis),
           channel: Keyword.get(broadcast, :channel)}
        ]

      :redisx ->
        listener_opts =
          [instance: name, redis: Keyword.fetch!(broadcast, :redis)] ++
            Keyword.take(broadcast, [:stream, :group, :block_ms, :consumer])

        [{Embassy.Rails.RedisStreamListener, listener_opts}]

      _ ->
        []
    end
  end
end
