# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

(no unreleased changes)

## [0.2.0] - 2026-06-16

### Added

- `Embassy.Rails.RedisListener`: AnyCable Redis broadcaster source.
  Configure with `broadcast: [source: :redis, redis: <url-or-opts>,
  channel: "__anycable__"]`. Requires the optional `:redix` dependency.
- `:telemetry` events `[:embassy_rails, :broadcast, :redis, :connected
  | :disconnected | :exception]`.

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
