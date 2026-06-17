# AnyCable feature gaps

Known gaps between the embassy_rails implementation and the full AnyCable
feature surface. Ordered by real-world impact.

## High impact

### Multi-node broadcast uses local_broadcast

`Embassy.Rails.Broadcast.dispatch/3` calls `Phoenix.PubSub.local_broadcast/3`,
which only delivers to WebSocket handlers on the current BEAM node. In a
multi-node deployment a broadcast arriving at node A over HTTP or Redis will
not reach connections on node B.

Fix: replace with `Phoenix.PubSub.broadcast/3`. The reason `local_broadcast`
was used in v0.1 is that it avoids a double-delivery race if all nodes share
the same Redis source — but the correct solution is to use a PubSub adapter
that is not Redis (e.g. `Phoenix.PubSub` with the default PG2 adapter) for
intra-cluster fan-out, while Redis is only used for the inbound broadcast
source. Using `broadcast/3` unconditionally is the right call once multi-node
is in scope.

## Medium impact

### wss:// scheme incorrect behind TLS-terminating proxies

`Embassy.Rails.Upgrade.build_url/1` checks `conn.scheme == :https` to emit
`wss://` vs `ws://`. Behind nginx, an ALB, or any TLS-terminating reverse
proxy, `conn.scheme` is `:http` even though the client connected over TLS.

Fix: consult `x-forwarded-proto` / `x-forwarded-ssl` headers before falling
back to `conn.scheme`. The `Plug.Conn.get_req_header/2` approach is
straightforward; the consumer can also set `plug :put_secure_browser_headers`
or configure the endpoint with `force_ssl`.

Consequence: Rails channel code that inspects `env.url` or
`request.env["HTTP_URL"]` sees `ws://` for TLS connections routed through a
proxy, which can break auth or CSP checks in the Rails app.

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

## Out of scope (AnyCable Pro)

The following features are part of AnyCable Pro and require a separate proto
service (`History`, `Presence`). They are not in the vendored
`priv/protos/anycable_rpc.proto` and are not planned for embassy_rails.

- **Channel history / stream history** — `GetHistory` RPC; replay missed
  messages on reconnect.
- **Presence** — `Presence` service; track online members per channel.

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
