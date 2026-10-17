# Embassy.Rails

Rails / ActionCable / AnyCable adapter sibling to the `embassy` hex
package. Provides WebSocket termination + HTTP RPC bridge to a Rails
backend. Extracted from `../emissary_example/phoenix_app/`.

## The family

These are independent hex packages composed at the consumer level.

- **`embassy`** (`../embassy`) — Phoenix-as-front-door HTTP reverse
  proxy + per-instance config harness. At v0.3.0-dev, not yet
  published to Hex.
- **`embassy_rails`** (this repo) — Rails-specific WS terminator +
  AnyCable bridge. Depends on `embassy` for per-instance config and
  the parent's Finch.
- **`attache`** — subprocess supervision (Puma master under OTP).
  Independent; this repo does not depend on it.
- **`embassy_example`** — the eventual demo app combining all three.

## This repo is a library, not an application

- **No `config/dev.exs`, `config/runtime.exs`, or app-specific env
  config.** Configuration is per-instance via the `{Embassy.Rails, ...}`
  child spec.
- **No Phoenix endpoint, no Rails dep, no `Phoenix.PubSub` instance.**
  The consumer owns those; embassy_rails plugs into them.
- **No demo or runnable app under `lib/`, `priv/`, or `scripts/`.**
- **Tests don't boot a Phoenix server, don't run Rails, don't talk to
  a real AnyCable server.** They exercise plugs, callbacks, and the
  RPC client with `Bypass`.

## Project conventions

- Branch off `main` for extraction work. Initial skeleton + spec
  commits are on `main`; everything else lives on `feature/*`.
- **No authorship attribution in commits.** No `Co-Authored-By`
  trailers, no Claude attribution.
- Elixir 1.19+.
- **Dialyxir is manual only.** Run `mix dialyzer` by hand; it is not part
  of `mix ci` or GitHub Actions.
- No emojis anywhere — code, comments, commit messages, docs.
- The user controls when commits happen.

## Status

Extraction is done and the package has grown past its original spec:
`lib/embassy/rails/` has the WS upgrade/connection, RPC client (HTTP
*and* a gRPC transport with generated protobuf stubs, not in the
original design), broadcast + broadcast plug, a Redis broadcast
listener, and per-instance config — at v0.3.0-dev with 13 test files.
Not yet published to Hex. Still depends on `embassy` via the spacebat
local Hex registry (switched off a path dep 2026-06-18), not path. The
demo app, `emissary_example`, was never switched over to depend on
either this package or `embassy` — see `embassy/CLAUDE.md`'s own
Status section for that gap.

## Composition reminder

`Embassy.Rails` reads `:finch` from the parent `Embassy` instance via
`Embassy.Config.snapshot/1`. It does not declare its own Finch. It does
require the consumer to pass `:pubsub` (a `Phoenix.PubSub` name) because
embassy itself doesn't carry PubSub.

## Read first

- `docs/superpowers/specs/2026-05-19-embassy-rails-v0.1-design.md` —
  full design spec
- `docs/superpowers/plans/2026-05-19-embassy-rails-v0.1.md` —
  implementation plan
- `../embassy/CLAUDE.md` — sibling package conventions
- `../emissary_example/docs/ANYCABLE_BRIDGE.md` — ~1100-line design
  history covering the AnyCable protocol, escalation phases, and the
  rationale behind every architecture choice
