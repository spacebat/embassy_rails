defmodule Embassy.Rails.Test.TelemetryCollector do
  @moduledoc false

  @doc """
  Attach a handler for the given event names. Returns `{:ok, ref_map}`
  whose ref is also the handler-id. Auto-detaches when the calling
  test process exits via `ExUnit.Callbacks.on_exit/1`.
  """
  def attach(event_names) when is_list(event_names) do
    ref = make_ref()
    {:ok, agent} = Agent.start_link(fn -> [] end)

    :telemetry.attach_many(
      ref,
      event_names,
      &__MODULE__.__handle__/4,
      agent
    )

    ExUnit.Callbacks.on_exit(fn ->
      :telemetry.detach(ref)
    end)

    {:ok, %{ref: ref, agent: agent}}
  end

  @doc """
  Return all events captured so far, in arrival order. Each element is
  `{event_name, measurements, metadata}`.
  """
  def events(%{agent: agent}) do
    agent |> Agent.get(& &1) |> Enum.reverse()
  end

  @doc false
  def __handle__(event_name, measurements, metadata, agent) do
    Agent.update(agent, &[{event_name, measurements, metadata} | &1])
  end
end
