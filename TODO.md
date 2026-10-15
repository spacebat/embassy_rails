# embassy_rails TODO

Tracked follow-up work. One bullet per item; move to the CHANGELOG when
done. AnyCable feature-parity gaps live separately in
`docs/anycable-gaps.md`.

Context: in a Kubernetes deployment, anycable-rpc and Rails are Services
and Kubernetes does supervision and scaling. These items are what
embassy_rails needs to behave well as a set of Phoenix pods in that
setting (replaces the earlier idea of a separate `ambassador` control
plane). See also the multi-node `local_broadcast` gap in
`docs/anycable-gaps.md`, which is a hard prerequisite for running more
than one Phoenix pod.

- **Graceful WS drain on shutdown** — when a Phoenix pod receives SIGTERM
  (rollout, scale-down), close its ActionCable connections gradually across
  a configurable window, not all at once when the VM stops. Send ActionCable's
  `{"type":"disconnect","reason":"server_restart","reconnect":true}` first,
  so clients reconnect to another pod. Without this, every rollout causes a
  reconnect storm, and each reconnect is an anycable-rpc `connect` call (plus
  re-subscribes), so RPC load spikes exactly while capacity is being
  swapped. Needs a drain coordinator in the supervision tree that stops new
  upgrades (fail readiness) and then closes connections from the
  `Embassy.Rails.Connection` registry in batches. It has to fit within
  `terminationGracePeriodSeconds`. Document the matching `preStop` hook.
- **`Embassy.Rails.Telemetry.metrics/0`** — `Telemetry.Metrics` definitions
  over the existing events (`[:embassy_rails, :rpc, op, :start | :stop]`,
  `[:embassy_rails, :rpc, :exception]`, `[:embassy_rails, :ws, :connection,
  :open | :close]`, `[:embassy_rails, :broadcast, ...]`), mirroring
  `Embassy.Telemetry.metrics/0`. Key series: RPC call rate and duration by
  `op` (the signal for scaling anycable-rpc through HPA/KEDA), RPC error
  count, open WS connection gauge (for scaling Phoenix pods), and broadcast
  received/rejected counts plus Redis connection state. `telemetry_metrics`
  as an optional dep.
- **RPC readiness check** — like `Embassy.Health`: report not-ready when
  anycable-rpc can't be reached (HTTP or gRPC transport), so a pod doesn't
  accept WS upgrades it can't authenticate. The drain coordinator above
  also flips this to not-ready. Keep it out of liveness.
