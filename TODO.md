# embassy_rails TODO

Tracked follow-up work. One bullet per item; move to the CHANGELOG when
done. AnyCable feature-parity gaps live separately in
`docs/anycable-gaps.md`.

Context: in a Kubernetes deployment, anycable-rpc and Rails are Services
and Kubernetes does supervision and scaling. These items are what
embassy_rails needs to behave well as a set of Phoenix pods in that
setting (replaces the earlier idea of a separate `ambassador` control
plane). Multi-node broadcast, graceful drain and readiness are done (see the
CHANGELOG).

- **`Embassy.Rails.Telemetry.metrics/0`** — `Telemetry.Metrics` definitions
  over the existing events (`[:embassy_rails, :rpc, op, :start | :stop]`,
  `[:embassy_rails, :rpc, :exception]`, `[:embassy_rails, :ws, :connection,
  :open | :close]`, `[:embassy_rails, :broadcast, ...]`), mirroring
  `Embassy.Telemetry.metrics/0`. Key series: RPC call rate and duration by
  `op` (the signal for scaling anycable-rpc through HPA/KEDA), RPC error
  count, open WS connection gauge (for scaling Phoenix pods), and broadcast
  received/rejected counts plus Redis connection state. `telemetry_metrics`
  as an optional dep.
