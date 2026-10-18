# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Requires `embassy ~> 0.3` (was `~> 0.2`). Only `Embassy.Config.snapshot/1`
  is used, so embassy 0.4 is accepted too.

### Added

- Telemetry `[:embassy_rails, :rpc, :grpc, :connected]` each time the gRPC
  channel is (re)established.

### Fixed

- `Embassy.Rails.GrpcChannel` leaked a connection on each reconnect, and
  a burst of failed calls opened one connection per call. It now closes
  the old channel, and a reconnect request for a channel that has already
  been replaced is ignored.

## [0.3.0] - 2026-10-18

### Added

- gRPC AnyCable RPC transport: `rpc: [transport: {:grpc, endpoint},
  secret: ...]`, where `endpoint` is `"host:port"`. Lets embassy_rails
  front a standard AnyCable Rails deployment whose RPC server speaks gRPC,
  with no HTTP RPC shim. Requires the optional `:grpc` and `:protobuf`
  dependencies. The transport is transparent to `Embassy.Rails.Connection`.
- `broadcast: [source: :redisx, redis: ...]`: AnyCable's Redis Streams
  broadcaster (`Embassy.Rails.RedisStreamListener`), with anycable-go's
  defaults (stream `"__anycable__"`, consumer group `"bx"`, 2000 ms block):
  `XREADGROUP`, `XAUTOCLAIM` of a dead consumer's entries, `XACK` + `XDEL`
  per entry, group recreated at `0` after `NOGROUP` or `UNBLOCKED` so
  nothing added meanwhile is lost. At-least-once delivery.
  `mix embassy_rails.install --source redisx [--stream ...]`.
- AnyCable presence (`Embassy.Rails.Presence`), cluster-wide on
  `Phoenix.Tracker`: `join_presence` / `leave_presence` from Rails, the
  ext-json client commands `join` / `leave` / `presence`, join and leave
  events, and a departure TTL so a quick reconnect is never seen as a
  leave. Option `presence: [ttl_ms: 60_000, api_secret: nil, upstream:
  nil]`, or `presence: false`.
- `Embassy.Rails.PresenceApiPlug`: anycable-go's `GET
  /api/presence/:stream/users`, with optional union with an upstream
  anycable-go (503 rather than a partial list when it fails).
- `actioncable-v1-ext-json` is offered by `Embassy.Rails.Upgrade` by
  default.
- `headers:` instance option: extra request headers forwarded to the RPC
  `Env` (case-insensitive).
- `rpc: [timeout: ms]` (default 5000) for both transports.
- `Embassy.Rails.Broadcast.scope/1`: `:redis` dispatches node-locally;
  `:http` and `:redisx` dispatch cluster-wide.
- Graceful WebSocket drain: `{Embassy.Rails.Drain, instance: ...,
  window_ms: 60_000, batches: 20}`, started after the Endpoint, sends every
  connection a reconnecting `server_restart` disconnect in batches across
  the window, waits one more interval for them to close, and flips
  readiness to false. `Embassy.Rails.Upgrade` answers 503 while draining.
- `Embassy.Rails.Health.ready?/1`: readiness probe, false while draining
  or when the AnyCable RPC is unreachable.
- `Embassy.Rails.connections_name/1`: duplicate-key Registry of live
  connections (key `:conn`).
- `ping_interval:` (default 3000 ms, `:infinity` disables),
  `max_mailbox:` (default 1000) and `slow_client_ms:` (default 5000; a
  client over `max_mailbox` that long is disconnected with `slow_client`)
  instance options, validated at start.
- `Embassy.Rails.Telemetry.metrics/0`: `Telemetry.Metrics` definitions
  (optional `:telemetry_metrics` dependency).
- Telemetry `[:embassy_rails, :broadcast, :redisx, _]` and
  `[:embassy_rails, :presence, :join | :leave]`; `[:embassy_rails,
  :broadcast, :received]` metadata gains `scope`.

### Changed

- `:http` broadcasts are re-broadcast cluster-wide (they were node-local),
  since Rails posts each one to a single load-balanced node. A multi-node
  deployment on the HTTP source now needs a clustered `Phoenix.PubSub`.
- Presence is on by default (`presence: false` turns it off).
- `Embassy.Rails.Upgrade` prefers `actioncable-v1-ext-json` when the
  client offers it (the default list was `["actioncable-v1-json"]`).
- `Embassy.Rails.RedisListener.decode_anycable/1` is removed; envelope
  decoding is internal and accepts batched (array) payloads.

### Fixed

- Batched AnyCable broadcasts (a JSON array of envelopes) were dropped by
  the `:http` and `:redis` sources; every element is now dispatched.
- Command replies carry only the `cstate` / `istate` keys that changed;
  they are now merged into the connection state instead of replacing it.
- `wss://` is reported to the RPC behind a TLS-terminating proxy
  (`x-forwarded-proto`, including a comma list, and `x-forwarded-ssl`).
- Channel `istate` is kept per channel identifier, as anycable-go does,
  instead of one map shared by every channel on the connection.
- `Embassy.Rails.BroadcastPlug` compares the bearer in constant time.

## [0.2.0] - 2026-06-16

### Added

- `Embassy.Rails.RedisListener`: AnyCable Redis broadcaster source.
  Configure with `broadcast: [source: :redis, redis: <url-or-opts>,
  channel: "__anycable__"]`. Requires the optional `:redix` dependency.
- `:telemetry` events `[:embassy_rails, :broadcast, :redis, :connected
  | :disconnected | :exception]`.
- `mix embassy_rails.install`: Igniter installer that wires Embassy.Rails
  into a Phoenix app (composes `embassy.install`, adds the PubSub +
  `Embassy.Rails` child and the `/cable`, `/_broadcast`, `/` routes).
  Requires the optional `:igniter` dependency.

## [0.1.0] - 2026-05-19

### Added

- `Embassy.Rails` supervisor and `child_spec/1` for consumer-tree mounting,
  composing with a parent `Embassy` instance.
- `Embassy.Rails.Config` per-instance snapshot accessor backed by Registry meta.
- `Embassy.Rails.Upgrade` WS upgrade plug with subprotocol negotiation
  (defaults to `actioncable-v1-json`).
- `Embassy.Rails.Connection` `@behaviour WebSock` handler that terminates
  the ActionCable WS and bridges to Rails via AnyCable HTTP RPC.
- `Embassy.Rails.RPC` AnyCable HTTP RPC client (connect/command/disconnect)
  using the parent embassy's Finch and transport.
- `Embassy.Rails.Broadcast` `Phoenix.PubSub` dispatch helper.
- `Embassy.Rails.BroadcastPlug` HTTP `/_broadcast` endpoint Plug.
- `:telemetry` events for RPC, broadcast, upgrade, and WS lifecycle.
