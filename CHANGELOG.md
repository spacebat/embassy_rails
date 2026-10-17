# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- gRPC AnyCable RPC transport: `rpc: [transport: {:grpc, endpoint},
  secret: ...]`, where `endpoint` is `"host:port"`. Lets embassy_rails
  front a standard AnyCable Rails deployment whose RPC server speaks gRPC,
  with no HTTP RPC shim. Requires the optional `:grpc` and `:protobuf`
  dependencies. The transport is transparent to `Embassy.Rails.Connection`.

- Graceful WebSocket drain: `{Embassy.Rails.Drain, instance: ...,
  window_ms: 60_000, batches: 20}`, started after the Endpoint, sends every
  connection a reconnecting `server_restart` disconnect in batches across
  the window, and flips readiness to false.
- `Embassy.Rails.Health.ready?/1`: readiness probe, false while draining
  or when the AnyCable RPC is unreachable.
- `Embassy.Rails.connections_name/1`: duplicate-key Registry of live
  connections (key `:conn`).
- `ping_interval:` (default 3000 ms, `:infinity` disables) and
  `max_mailbox:` (default 1000; slow clients are disconnected with
  `slow_client`) instance options.

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
