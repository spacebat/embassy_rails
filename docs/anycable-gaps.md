# AnyCable feature gaps

Known gaps between the embassy_rails implementation and the full AnyCable
feature surface. Ordered by real-world impact.

## High impact

### Multi-node broadcast uses local_broadcast for the HTTP source

`Embassy.Rails.Broadcast.dispatch/4` takes a scope. The `:redisx` source
dispatches with `:cluster` (`Phoenix.PubSub.broadcast/3`), because each
stream entry is read by one node. The `:redis` source keeps `:local`, which
is correct: every node subscribes to the Pub/Sub channel. The `:http` source
also uses `:local`, so a broadcast posted to node A over HTTP will not reach
connections on node B unless Rails posts to every node.

Fix: dispatch the `:http` source with `:cluster` scope when Rails posts to a
single load-balanced endpoint (one node receives each broadcast). Keep
`:local` when Rails posts to every node. This likely wants a per-instance
option, since embassy_rails cannot tell which topology it is in.

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
