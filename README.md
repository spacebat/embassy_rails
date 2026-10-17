# Embassy.Rails

Rails / ActionCable / AnyCable adapter for the embassy hex package.
Terminates ActionCable WebSockets in Phoenix and bridges to a Rails
backend via AnyCable's HTTP RPC and broadcast protocols.

`embassy_rails` composes with `embassy` (the HTTP reverse proxy +
per-instance config harness) and `Phoenix.PubSub` to form a complete
Phoenix-as-front-door deployment for Rails.

## What ships in v0.3

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
- **`Embassy.Rails.RedisListener`** — AnyCable Redis Pub/Sub broadcaster source.
- **`Embassy.Rails.RedisStreamListener`** — AnyCable Redis Streams
  (`redisx`) broadcaster source.
- **`Embassy.Rails.Presence`** and **`Embassy.Rails.PresenceApiPlug`** —
  AnyCable presence and its REST API.
- **`Embassy.Rails`** supervisor + **`Embassy.Rails.Config`** — per-
  instance plumbing.
- `:telemetry` for RPC, broadcast, upgrade, and WS lifecycle.

## Not yet shipped

- Generic `Embassy.Upgrade` extraction (this lives in
  `Embassy.Rails.Upgrade` for now; will migrate non-breaking).

## Broadcast sources

Broadcasts from Rails reach connected clients via one of three sources,
selected per instance with `broadcast: [source: ...]`:

- `:http` — AnyCable's HTTP broadcaster posts to `Embassy.Rails.BroadcastPlug`.
  Rails posts each broadcast to one (load-balanced) node, which
  re-broadcasts it cluster-wide, so a multi-node deployment needs a
  clustered `Phoenix.PubSub`.
- `:redisx` — `Embassy.Rails.RedisStreamListener` consumes AnyCable's Redis
  Streams broadcaster, with the same defaults as anycable-go (stream
  `"__anycable__"`, consumer group `"bx"`). In Rails, set
  `broadcast_adapter: redisx` in `config/anycable.yml` (or
  `ANYCABLE_BROADCAST_ADAPTER=redisx`). Delivery is at-least-once and
  survives a terminator restart. Each broadcast is read by one terminator
  node, which re-broadcasts it cluster-wide, so a multi-node deployment
  needs a clustered `Phoenix.PubSub`. Requires Redis 6.2+ and the
  optional `:redix` dependency. Prefer this over `:redis` for new Redis
  setups. Each entry is deleted once read, by this listener and by
  anycable-go alike, so a stream must have one reader: do not run
  anycable-go and Embassy.Rails on the same stream, even in different
  consumer groups. Cut a stream over in one step.
- `:redis` — `Embassy.Rails.RedisListener` subscribes to AnyCable's Redis
  Pub/Sub broadcaster channel (default `"__anycable__"`; Rails
  `broadcast_adapter: redis`). Fire-and-forget: a broadcast published
  while a node is disconnected is lost. Every node subscribes, so no
  clustered PubSub is needed. Requires the optional `:redix` dependency.

All three accept AnyCable's batched broadcasts (a JSON array of
envelopes). Remote commands such as `remote_connections.disconnect` are
received but ignored.

## Presence

AnyCable presence works as in anycable-go, cluster-wide on `Phoenix.Tracker`
(multi-node needs a clustered `Phoenix.PubSub`). It is on by default:

    presence: [ttl_ms: 60_000, api_secret: nil, upstream: nil]   # or presence: false

- In Rails, `include AnyCable::Rails::Channel::Presence` and call
  `join_presence` / `leave_presence` from a channel.
- JS clients use `protocol: "actioncable-v1-ext-json"` for
  `channel.presence.join/leave/info` and presence events.
- A disconnected client stays present for `ttl_ms`, so a quick reconnect is
  never seen as a leave. A node that dies is kept on the others for
  `ttl_ms` too (the tracker's `down_period`).
- `Embassy.Rails.PresenceApiPlug` serves anycable-go's REST API:

      forward "/api/presence", Embassy.Rails.PresenceApiPlug,
        embassy_rails: MyApp.Embassy.Rails

  `GET /api/presence/:stream/users`, bearer `api_secret`. With no
  `api_secret` (the default) the endpoint is open to anyone who can reach
  it, and lists the presence ids of any stream: set one, or keep the route
  off the public network. With `upstream:`
  set to an anycable-go base URL, its records are unioned in, and an upstream
  failure answers 503 rather than a partial list.

## Installer

With the optional `:igniter` dependency present, wire Embassy.Rails into a
Phoenix app in one command:

    mix igniter.install embassy_rails

It composes embassy's installer for the Finch + Embassy base, then adds a
`Phoenix.PubSub`, an `Embassy.Rails` instance, and a router scope
(`/cable` WebSocket upgrade, an optional `/_broadcast` endpoint for the
HTTP broadcast source, and the `/` reverse proxy). Use
`--source redisx` (Redis Streams, with an optional `--stream`) or
`--source redis` (Pub/Sub) to wire a Redis broadcast source instead
(adds `{:redix, "~> 1.0"}`). Pass `--yes` for non-interactive defaults.

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

### gRPC RPC transport

To front a standard AnyCable deployment whose RPC server speaks gRPC, use
the `:grpc` transport instead of `:uds`/`:http`:

    rpc: [transport: {:grpc, "localhost:50051"}, secret: "rpc-secret"]

This requires the optional `:grpc` and `:protobuf` dependencies:

    {:grpc, "~> 1.0"},
    {:protobuf, "~> 0.17"}

The same `:secret` is sent as gRPC `authorization: Bearer` metadata, so a
Rails app configured with one AnyCable RPC secret works across the HTTP
and gRPC transports unchanged.

## Telemetry

Subscribe to `:telemetry` events with the `[:embassy_rails | _]` prefix
for RPC timing, broadcast volume, upgrade rejections, and WS lifecycle
counts. With the optional `:telemetry_metrics` dependency,
`Embassy.Rails.Telemetry.metrics/0` gives ready-made `Telemetry.Metrics`
definitions for a reporter.

## Operations

- `{Embassy.Rails.Drain, instance: MyApp.Embassy.Rails, window_ms: 30_000}`,
  placed after the Endpoint in the supervision tree, closes connections in
  batches with a reconnecting `server_restart` disconnect on shutdown, then
  waits up to one batch interval for them to close. New upgrades get a 503
  while it drains. Keep the window plus one interval inside
  `terminationGracePeriodSeconds`.
- `Embassy.Rails.Health.ready?/1` for the readiness probe (not liveness).
- `ping_interval:`, `max_mailbox:` and `slow_client_ms:` instance options
  tune pings and the slow-client cutoff: a connection is closed with
  `slow_client` once its mailbox has stayed over `max_mailbox` for
  `slow_client_ms` (default 5s), so a burst that drains is still delivered.

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
