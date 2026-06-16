if Code.ensure_loaded?(Igniter.Mix.Task) do
  defmodule Mix.Tasks.EmbassyRails.Install do
    @shortdoc "Wires Embassy.Rails into a Phoenix application"
    @moduledoc """
    Installs Embassy.Rails on top of the embassy base.

    Composes `embassy.install --no-proxy --no-cable` for the Finch +
    Embassy + endpoint body-parser base, then adds a `Phoenix.PubSub`,
    an `Embassy.Rails` instance, and a router scope: `/cable` (WS
    upgrade), an optional `/_broadcast` HTTP endpoint, and the `/`
    reverse proxy - in that order.

    Flags: `--name`, `--embassy`, `--pubsub`, `--finch`,
    `--transport uds|http`, `--address`, `--secret`,
    `--source http|redis`, `--key`, `--redis-url`, `--channel`. `--yes`
    takes defaults non-interactively.
    """

    use Igniter.Mix.Task

    @example "mix igniter.install embassy_rails"
    @placeholder_secret "REPLACE_WITH_ANYCABLE_RPC_SECRET"
    @placeholder_key "REPLACE_WITH_BROADCAST_KEY"
    @default_channel "__anycable__"

    @impl Igniter.Mix.Task
    def info(_argv, _composing_task) do
      %Igniter.Mix.Task.Info{
        group: :embassy_rails,
        example: @example,
        composes: ["embassy.install"],
        schema: [
          name: :string,
          embassy: :string,
          pubsub: :string,
          finch: :string,
          transport: :string,
          address: :string,
          secret: :string,
          source: :string,
          key: :string,
          redis_url: :string,
          channel: :string,
          yes: :boolean
        ]
      }
    end

    @impl Igniter.Mix.Task
    def igniter(igniter) do
      {igniter, opts} = resolve_opts(igniter)

      igniter
      |> compose_base(opts)
      |> add_supervision_tree(opts)
    end

    # --- option resolution -----------------------------------------------

    defp resolve_opts(igniter) do
      options = igniter.args.options
      yes? = Keyword.get(options, :yes, false)
      prefix = Igniter.Project.Module.module_name_prefix(igniter)

      {igniter, router} =
        Igniter.Libs.Phoenix.select_router(
          igniter,
          "Which router should Embassy.Rails mount into?"
        )

      {igniter, endpoint} =
        Igniter.Libs.Phoenix.select_endpoint(igniter, router, "Which endpoint owns that router?")

      transport = resolve(options, :transport, "uds", "Transport (uds/http)", yes?)
      address = resolve(options, :address, default_address(transport), "Backend address", yes?)
      secret = resolve(options, :secret, @placeholder_secret, "AnyCable RPC secret", yes?)
      source = resolve(options, :source, "http", "Broadcast source (http/redis)", yes?)

      name =
        parse_module(
          resolve(
            options,
            :name,
            default_name(prefix, "Embassy.Rails"),
            "Embassy.Rails instance name",
            yes?
          )
        )

      embassy =
        parse_module(
          resolve(
            options,
            :embassy,
            default_name(prefix, "Embassy"),
            "Embassy instance name",
            yes?
          )
        )

      pubsub =
        parse_module(
          resolve(options, :pubsub, default_name(prefix, "PubSub"), "Phoenix.PubSub name", yes?)
        )

      finch =
        parse_module(resolve(options, :finch, default_name(prefix, "Finch"), "Finch name", yes?))

      {broadcast, key} = resolve_broadcast(options, source, yes?)

      opts = %{
        prefix: prefix,
        router: router,
        endpoint: endpoint,
        name: name,
        embassy: embassy,
        pubsub: pubsub,
        finch: finch,
        transport: transport,
        address: address,
        secret: secret,
        source: source,
        broadcast: broadcast,
        key: key
      }

      {igniter, opts}
    end

    defp resolve_broadcast(options, "redis", yes?) do
      redis_url = resolve(options, :redis_url, "redis://localhost:6379/1", "Redis URL", yes?)

      broadcast =
        case Keyword.get(options, :channel) do
          nil -> [source: :redis, redis: redis_url]
          channel -> [source: :redis, redis: redis_url, channel: channel]
        end

      {broadcast, nil}
    end

    defp resolve_broadcast(options, _http, yes?) do
      key =
        case Keyword.get(options, :key) do
          nil ->
            if yes?, do: @placeholder_key, else: prompt_default("Broadcast key", generated_key())

          value ->
            value
        end

      {[source: :http, key: key], key}
    end

    defp resolve(options, key, default, label, yes?) do
      case Keyword.get(options, key) do
        nil -> if yes?, do: default, else: prompt_default(label, default)
        value -> value
      end
    end

    defp prompt_default(label, default) do
      case String.trim(Mix.shell().prompt("#{label} [#{default}]: ")) do
        "" -> default
        value -> value
      end
    end

    defp generated_key, do: :crypto.strong_rand_bytes(24) |> Base.url_encode64(padding: false)

    defp default_name(prefix, suffix), do: inspect(Module.concat(prefix, suffix))
    defp default_address("http"), do: "http://localhost:4000"
    defp default_address(_uds), do: "/tmp/puma.sock"
    defp parse_module(string), do: Module.concat([string])

    defp transport_atom("http"), do: :http
    defp transport_atom(_uds), do: :uds

    defp transport_tuple(opts),
      do: "{#{inspect(transport_atom(opts.transport))}, #{inspect(opts.address)}}"

    # --- supervision tree ------------------------------------------------

    defp add_supervision_tree(igniter, opts) do
      # Phoenix apps usually already declare a Phoenix.PubSub child; add_new_child
      # no-ops on the module conflict. Embassy.Rails validates the parent Embassy
      # instance synchronously at start, so it must boot after Embassy (and after
      # PubSub, which it uses for fan-out): place it with `after:`.
      igniter
      |> Igniter.Project.Application.add_new_child({Phoenix.PubSub, name: opts.pubsub},
        after: [Embassy]
      )
      |> Igniter.Project.Application.add_new_child(
        {Embassy.Rails,
         name: opts.name,
         embassy: opts.embassy,
         pubsub: opts.pubsub,
         rpc: [transport: {transport_atom(opts.transport), opts.address}, secret: opts.secret],
         broadcast: opts.broadcast},
        after: [Phoenix.PubSub, Embassy]
      )
    end

    # --- base composition ------------------------------------------------

    defp compose_base(igniter, opts) do
      Igniter.compose_task(igniter, "embassy.install", [
        "--yes",
        "--name",
        inspect(opts.embassy),
        "--finch",
        inspect(opts.finch),
        "--transport",
        opts.transport,
        "--address",
        opts.address,
        "--no-proxy",
        "--no-cable"
      ])
    end
  end
else
  defmodule Mix.Tasks.EmbassyRails.Install do
    @shortdoc "Wires Embassy.Rails into a Phoenix application (requires igniter)"
    @moduledoc false
    use Mix.Task

    @impl Mix.Task
    def run(_argv) do
      Mix.raise("""
      The embassy_rails.install task requires the optional :igniter
      dependency. Add it to your deps and run mix deps.get:

          {:igniter, "~> 0.6", only: [:dev, :test]}
      """)
    end
  end
end
