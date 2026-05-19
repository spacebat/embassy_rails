defmodule Embassy.Rails.Connection do
  @moduledoc """
  `WebSock` handler that terminates an ActionCable WebSocket and
  proxies the protocol to Rails via AnyCable HTTP RPC.

  Instantiated by `Embassy.Rails.Upgrade` via
  `WebSockAdapter.upgrade/4`. The state map carries the embassy_rails
  instance name, the AnyCable connection identifiers + state, and the
  active stream subscriptions.

  See the v0.1 spec at
  `docs/superpowers/specs/2026-05-19-embassy-rails-v0.1-design.md` for
  the design rationale and error-handling matrix.
  """

  @behaviour WebSock

  alias Embassy.Rails.RPC

  require Logger

  @ping_interval 3_000

  @impl WebSock
  def init(state) do
    case RPC.connect(state.instance, build_env(state)) do
      {:ok, %{"status" => 1} = resp} ->
        state =
          %{
            state
            | identifiers: resp["identifiers"],
              cstate: get_in(resp, ["env", "cstate"]) || %{},
              istate: get_in(resp, ["env", "istate"]) || %{}
          }

        schedule_ping()
        {:push, connect_frames(resp["transmissions"] || []), state}

      {:ok, %{"status" => 2}} ->
        disconnect_frame =
          {:text, Jason.encode!(%{type: "disconnect", reason: "unauthorized", reconnect: false})}

        {:stop, :normal, {1011, "unauthorized"}, [disconnect_frame], state}

      {:error, reason} ->
        Logger.error("Embassy.Rails.Connection: connect RPC failed: #{inspect(reason)}")
        {:stop, :normal, {1011, "rpc_error"}, state}
    end
  end

  @impl WebSock
  def handle_in({_payload, [opcode: opcode]}, state) when opcode != :text do
    Logger.debug("Embassy.Rails.Connection: dropping #{opcode} frame")
    {:ok, state}
  end

  def handle_in({_payload, [opcode: :text]}, state) do
    # Command handling lands in Task 10.
    {:ok, state}
  end

  @impl WebSock
  def handle_info(:ping, state) do
    frame = {:text, Jason.encode!(%{type: "ping", message: System.system_time(:second)})}
    schedule_ping()
    {:push, frame, state}
  end

  def handle_info(msg, state) do
    Logger.debug("Embassy.Rails.Connection: ignoring info: #{inspect(msg)}")
    {:ok, state}
  end

  @impl WebSock
  def terminate(_reason, %{identifiers: nil}), do: :ok

  def terminate(_reason, state) do
    RPC.disconnect(state.instance, %{
      identifiers: state.identifiers,
      subscriptions: state.streams |> Map.values() |> Enum.uniq(),
      env: build_env(state)
    })

    :ok
  end

  # --- private helpers ---

  defp build_env(state) do
    %{url: state.url, headers: state.headers, cstate: state.cstate, istate: state.istate}
  end

  defp connect_frames([first | _] = transmissions) do
    if welcome?(first),
      do: Enum.map(transmissions, &{:text, &1}),
      else: Enum.map(transmissions, &{:text, &1}) ++ [welcome_frame()]
  end

  defp connect_frames([]), do: [welcome_frame()]

  defp welcome_frame, do: {:text, Jason.encode!(%{type: "welcome"})}

  defp welcome?(transmission) do
    match?({:ok, %{"type" => "welcome"}}, Jason.decode(transmission))
  end

  defp schedule_ping, do: Process.send_after(self(), :ping, @ping_interval)
end
