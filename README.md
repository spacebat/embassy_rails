# Embassy.Rails

Rails / ActionCable / AnyCable adapter for the embassy hex package.
Terminates ActionCable WebSockets in Phoenix and bridges to a Rails
backend via AnyCable's HTTP RPC and broadcast protocols.

`embassy_rails` composes with `embassy` (the HTTP reverse proxy +
per-instance config harness) and `Phoenix.PubSub` to form a complete
Phoenix-as-front-door deployment for Rails.

## What ships in v0.1

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
- **`Embassy.Rails`** supervisor + **`Embassy.Rails.Config`** — per-
  instance plumbing.
- `:telemetry` for RPC, broadcast, upgrade, and WS lifecycle.

## Not in v0.1

- Redis broadcast source (HTTP-only; Redis comes in v0.2).
- Generic `Embassy.Upgrade` extraction (this lives in
  `Embassy.Rails.Upgrade` for now; will migrate non-breaking).
- gRPC + Protobuf RPC transport.

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

v0.1.0-dev. Not yet published to Hex.

## License

MIT. See `LICENSE`.
