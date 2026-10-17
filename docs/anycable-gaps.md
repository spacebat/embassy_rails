# AnyCable feature gaps

Known gaps between the embassy_rails implementation and the full AnyCable
feature surface. Ordered by real-world impact.

## Low impact

### socket_id not forwarded to the RPC Env

`Embassy.Rails.Upgrade` generates a `socket_id` per connection (the `"phx-…"`
random string) and stores it in connection state for `exclude_socket`
filtering. It is not sent to the AnyCable RPC in the `Env`. The base AnyCable
proto does not have a `socket_id` field; `anycable-go` propagates it through
its own internals.

Consequence: Rails channel code cannot read the originating socket ID from
`env` during `connect`/`subscribed` callbacks. For most apps this is invisible;
it matters if the Rails app uses `ActionCable.server.broadcast` with `:except`
targeting the originating socket.

### Ping interval not configurable

`Embassy.Rails.Connection` hard-codes `@ping_interval 3_000`. Some deployments
(behind ALBs with 60 s idle timeouts, or clients with aggressive battery
savings) need a different interval. Others may want to disable pings entirely
and rely on the proxy or client.

Fix: accept an optional `:ping_interval` key in the `Embassy.Rails` child spec
(`:infinity` to disable), defaulting to `3_000`.

### Installer does not offer the gRPC transport

`mix embassy_rails.install` (the Igniter task) only wires up `{:uds, …}` and
`{:http, …}`. The `:grpc` transport requires the consumer to add the deps and
configure manually.

Fix: add a `--transport grpc` flag to the installer task.

## Not implemented

Presence is implemented (`Embassy.Rails.Presence`, open source in anycable-go
since 1.5). These neighbouring AnyCable features are not:

- **Reliable streams / history** — the broker's stream history; ext-json
  clients' `history` requests are ignored, and the welcome message carries no
  `sid`, so there is no session recovery after a reconnect.
- **Signed and public streams** (`$pubsub`), including presence on them
  (`turbo-cable-presence-source`).
- **Whisper** (`$w`).
- **Presence webhooks** — not implemented in anycable-go either.

## Marginal

### Binary / MessagePack frames

`Embassy.Rails.Connection.handle_in/2` logs and drops any non-`:text` opcode.
The `actioncable-v1-msgpack` subprotocol is not advertised and binary frames
are not decoded. This is acceptable for all JSON-only workloads (the vast
majority of AnyCable deployments). Mention in docs so consumers are not
surprised.

### Multi-channel istate at disconnect

When a connection with multiple active subscriptions terminates, `terminate/2`
sends one `Disconnect` RPC with the connection-level `env` (carrying the last
channel's `istate`). This matches how `anycable-go` behaves. For Rails channels
that rely on per-channel `istate` at teardown, the istate for all channels
except the last will be wrong. No known real-world impact.
