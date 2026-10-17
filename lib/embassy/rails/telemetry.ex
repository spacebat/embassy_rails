if Code.ensure_loaded?(Telemetry.Metrics) do
  defmodule Embassy.Rails.Telemetry do
    @moduledoc """
    `Telemetry.Metrics` definitions over embassy_rails events. Hand them to a
    reporter (e.g. `TelemetryMetricsPrometheus.Core`). Tags are low
    cardinality: never the stream name.

    Requires the optional `:telemetry_metrics` dependency.

    The open-connection gauge is not an event: report
    `Registry.count(Embassy.Rails.connections_name(instance))` from a
    `:telemetry_poller` measurement in the consuming app.
    """

    import Telemetry.Metrics

    @spec metrics() :: [Telemetry.Metrics.t()]
    def metrics do
      rpc =
        for op <- [:connect, :command, :disconnect] do
          distribution("embassy_rails.rpc.#{op}.duration",
            event_name: [:embassy_rails, :rpc, op, :stop],
            measurement: :duration,
            unit: {:native, :millisecond},
            tags: [:instance],
            reporter_options: [buckets: [5, 10, 25, 50, 100, 250, 500, 1000, 5000]]
          )
        end

      rpc ++
        [
          counter("embassy_rails.rpc.exception.count",
            event_name: [:embassy_rails, :rpc, :exception],
            tags: [:instance, :kind]
          ),
          counter("embassy_rails.ws.open.count",
            event_name: [:embassy_rails, :ws, :connection, :open],
            tags: [:instance]
          ),
          counter("embassy_rails.ws.close.count",
            event_name: [:embassy_rails, :ws, :connection, :close],
            tags: [:instance]
          ),
          counter("embassy_rails.broadcast.received.count",
            event_name: [:embassy_rails, :broadcast, :received],
            tags: [:instance, :scope]
          ),
          counter("embassy_rails.broadcast.redisx.disconnected.count",
            event_name: [:embassy_rails, :broadcast, :redisx, :disconnected],
            tags: [:instance]
          ),
          counter("embassy_rails.broadcast.redisx.exception.count",
            event_name: [:embassy_rails, :broadcast, :redisx, :exception],
            tags: [:instance, :kind]
          ),
          sum("embassy_rails.broadcast.redisx.claimed.count",
            event_name: [:embassy_rails, :broadcast, :redisx, :claimed],
            measurement: :count,
            tags: [:instance]
          ),
          counter("embassy_rails.presence.join.count",
            event_name: [:embassy_rails, :presence, :join],
            tags: [:instance]
          ),
          counter("embassy_rails.presence.leave.count",
            event_name: [:embassy_rails, :presence, :leave],
            tags: [:instance]
          ),
          counter("embassy_rails.upgrade.rejected.count",
            event_name: [:embassy_rails, :upgrade, :rejected],
            tags: [:instance, :kind]
          )
        ]
    end
  end
end
