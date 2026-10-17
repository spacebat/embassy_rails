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

Nothing open: drain, readiness and metrics shipped in 0.3.0.
