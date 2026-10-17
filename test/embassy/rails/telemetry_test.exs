defmodule Embassy.Rails.TelemetryTest do
  use ExUnit.Case, async: true

  test "metrics cover rpc, ws, broadcast, redisx and presence events" do
    names = Enum.map(Embassy.Rails.Telemetry.metrics(), & &1.event_name)
    assert [:embassy_rails, :rpc, :command, :stop] in names
    assert [:embassy_rails, :rpc, :exception] in names
    assert [:embassy_rails, :ws, :connection, :open] in names
    assert [:embassy_rails, :broadcast, :received] in names
    assert [:embassy_rails, :broadcast, :redisx, :disconnected] in names
    assert [:embassy_rails, :presence, :join] in names
  end

  test "no metric is tagged with the stream name" do
    for metric <- Embassy.Rails.Telemetry.metrics() do
      refute :stream in metric.tags, "#{inspect(metric.name)} is tagged by stream"
    end
  end
end
