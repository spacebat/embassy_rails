# Embassy.Rails

Rails / ActionCable / AnyCable adapter for the embassy hex package.
Terminates ActionCable WebSockets in Phoenix and bridges to a Rails
backend via AnyCable's HTTP RPC and broadcast protocols.

`embassy_rails` composes with `embassy` (the HTTP reverse proxy +
per-instance config harness) and `Phoenix.PubSub` to form a complete
Phoenix-as-front-door deployment for Rails.

## What ships in v0.2

- **`Embassy.Rails.Upgrade`** — WS upgrade plug with subprotocol
  negotiation (defaults to `actioncable-v1-json`).
- **`Embassy.Rails.Connection`** — `@behaviour WebSock` handler that
  terminates the ActionCable WS and bridges to Rails over AnyCable HTTP
  RPC, including server-pushed broadcast delivery via PubSub.
- **`Embassy.Rails.RPC`** — AnyCable HTTP RPC client
  (`connect`/`command`/`disconnect`) using the parent embassy's Finch.
- **`Embassy.Rails.BroadcastPlug`** — HTTP `/_broadcast` endpoint Plug
  for AnyCable's broadcast adapter.
- **`Embassy.Rails.Broadcast`** — PubSub fan-out helper.
- **`Embassy.Rails.RedisListener`** — AnyCable Redis broadcaster source.
- **`Embassy.Rails`** supervisor + **`Embassy.Rails.Config`** — per-
  instance plumbing.
- `:telemetry` for RPC, broadcast, upgrade, and WS lifecycle.

## Not yet shipped

- Generic `Embassy.Upgrade` extraction (this lives in
  `Embassy.Rails.Upgrade` for now; will migrate non-breaking).
- gRPC + Protobuf RPC transport.

## Broadcast sources

Broadcasts from Rails reach connected clients via one of two sources,
selected per instance with `broadcast: [source: ...]`:

- `:http` — AnyCable's HTTP broadcaster posts to `Embassy.Rails.BroadcastPlug`.
- `:redis` — `Embassy.Rails.RedisListener` subscribes to AnyCable's Redis
  broadcaster channel (default `"__anycable__"`). Requires the optional
  `:redix` dependency. Useful when scaling terminator nodes horizontally
  or when Rails and the terminators should communicate only through Redis.

## Installer

With the optional `:igniter` dependency present, wire Embassy.Rails into a
Phoenix app in one command:

    mix igniter.install embassy_rails

It composes embassy's installer for the Finch + Embassy base, then adds a
`Phoenix.PubSub`, an `Embassy.Rails` instance, and a router scope
(`/cable` WebSocket upgrade, an optional `/_broadcast` endpoint for the
HTTP broadcast source, and the `/` reverse proxy). Use
`--source redis` to wire the Redis broadcast source instead (adds
`{:redix, "~> 1.0"}`). Pass `--yes` for non-interactive defaults.

## Installation

```elixir
def deps do
  [
    {:embassy, "~> 0.1"},
    {:embassy_rails, "~> 0.1"},
    {:phoenix_pubsub, "~> 2.1"}
  ]
end
```

## Quick wiring example

```elixir
# application.ex
children = [
  {Finch, name: MyApp.Finch},
  {Phoenix.PubSub, name: MyApp.PubSub},
  {Embassy, name: MyApp.Embassy, finch: MyApp.Finch},
  {Embassy.Rails,
     name: MyApp.Embassy.Rails,
     embassy: MyApp.Embassy,
     pubsub: MyApp.PubSub,
     rpc: [transport: {:uds, "/tmp/puma.sock"}, secret: "rpc-secret"],
     broadcast: [source: :http, key: "broadcast-bearer-token"]},
  MyAppWeb.Endpoint
]

Supervisor.start_link(children, strategy: :one_for_one)
```

```elixir
# router.ex
scope "/", MyAppWeb do
  pipe_through :api

  # WebSocket upgrade for ActionCable clients
  get "/cable", Embassy.Rails.Upgrade, embassy_rails: MyApp.Embassy.Rails

  # HTTP receiver for AnyCable's broadcast adapter
  post "/_broadcast", Embassy.Rails.BroadcastPlug, embassy_rails: MyApp.Embassy.Rails
end
```

## Telemetry

Subscribe to `:telemetry` events with the `[:embassy_rails | _]` prefix
for RPC timing, broadcast volume, upgrade rejections, and WS lifecycle
counts.

## Composition with `embassy`

`embassy_rails` reads its parent `embassy` instance's `:finch` via
`Embassy.Config.snapshot/1`, so you wire the Finch instance once into
`Embassy` and reuse it transparently for AnyCable RPC. The same
`embassy` instance can simultaneously reverse-proxy HTTP requests to
the same Rails backend via `Embassy.ReverseProxy` on a different route.

## Status

v0.2.0. Published to the local `spacebat` Hex registry (not public hex.pm).
See `RELEASING.md` for the publish process.

## License

MIT. See `LICENSE`.
