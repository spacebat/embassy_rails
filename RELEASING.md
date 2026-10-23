# Releasing embassy_rails

embassy_rails is published to the local `spacebat` Hex registry, not to
public hex.pm. See `~/.claude/skills/using-spacebat-hex-registry` for the
registry's layout and helper scripts.

## The embassy dependency gotcha

embassy_rails depends on its sibling `embassy`. In `mix.exs` that dependency
is **env-gated** (`embassy_dep/0`):

- By default: `{:embassy, "~> 0.4", repo: "spacebat"}`, a resolvable Hex
  requirement. This is what gets packaged.
- When `EMBASSY_PATH` is set: `{:embassy, path: "../embassy"}`, so edits to
  embassy flow in without a republish.

Hex rejects path deps, so the publish command **must not** have
`EMBASSY_PATH` set. With it set, `mix hex.build` fails on the path dep.

## Steps

1. Bump `@version` in `mix.exs` (drop any `-dev` suffix for a real release).
2. Update `CHANGELOG.md` and the README `## Status` line.
3. Confirm the tree is green: `mix ci` (it also compiles the package with
   `--no-optional-deps`, as a consumer without them would).
4. Make sure the registry server is running (needed during fetch/verify):
   `~/.hex/serve-spacebat-registry.sh` (port 8123).
5. Make sure the matching `embassy` version is already published to spacebat
   (embassy_rails resolves it from the registry at publish time).
6. Publish, with `EMBASSY_PATH` unset:

       ~/.hex/publish-to-spacebat.sh

   This runs `mix hex.build` (embassy resolves as a spacebat hex dep),
   drops the tarball into the registry, and rebuilds the signed index.

## Verify the published package

A package that builds is not necessarily one that *resolves and compiles*
for a consumer. Confirm with a throwaway project:

    cd /tmp && mix new consumer_check && cd consumer_check
    # add {:embassy_rails, "~> 0.4", repo: "spacebat"} to deps
    mix deps.get      # both embassy_rails and embassy resolve from spacebat
    mix compile       # compiles with none of the optional deps present
                      # (igniter, redix, grpc, protobuf, telemetry_metrics)

If the consumer compile fails on `use Igniter.Mix.Task`, the published
`embassy` is missing its `Code.ensure_loaded?` guard on the install task -
fix and republish embassy first.

## Consuming the release

    {:embassy, "~> 0.4", repo: "spacebat"},
    {:embassy_rails, "~> 0.4", repo: "spacebat"},
    {:phoenix_pubsub, "~> 2.1"}

Then `mix deps.get` with the registry server running. Add `{:igniter,
"~> 0.6"}` only if you want to run `mix embassy_rails.install`,
`{:redix, "~> 1.0"}` only for a Redis broadcast source, and
`{:grpc, "~> 1.0"}` plus `{:protobuf, "~> 0.17"}` only for the gRPC RPC
transport.
