defmodule Embassy.Rails.RPC do
  @moduledoc """
  HTTP RPC client speaking AnyCable's protocol to Rails.

  Transport is per-instance, read from the snapshot's `:rpc` opts:

    * `{:uds, socket_path}` — Finch dials a Unix domain socket; URL
      host is cosmetic.
    * `{:http, base_url}` — Finch dials a regular HTTP server.

  Three operations:

    * `connect/2` — opens a logical AnyCable connection.
    * `command/2` — one per inbound WS frame from the client.
    * `disconnect/2` — fire-and-forget on socket close.
  """

  alias Embassy.Rails.Config

  require Logger

  @timeout 5_000

  @spec connect(atom(), map()) :: {:ok, map()} | {:error, term()}
  def connect(instance, env), do: post(instance, :connect, "/_anycable/connect", %{env: env})

  @spec command(atom(), map()) :: {:ok, map()} | {:error, term()}
  def command(instance, payload), do: post(instance, :command, "/_anycable/command", payload)

  @spec disconnect(atom(), map()) :: :ok
  def disconnect(instance, payload) do
    Task.start(fn -> post(instance, :disconnect, "/_anycable/disconnect", payload) end)
    :ok
  end

  defp post(instance, op, path, body) do
    snap = Config.snapshot(instance)
    {url, finch_opts} = request_target(path, snap.rpc[:transport])

    headers = [
      {"content-type", "application/json"},
      {"authorization", "Bearer " <> snap.rpc[:secret]}
    ]

    start = System.monotonic_time()
    request_bytes = byte_size(Jason.encode!(body))

    :telemetry.execute(
      [:embassy_rails, :rpc, op, :start],
      %{monotonic_time: start, request_bytes: request_bytes},
      %{instance: instance, transport: snap.rpc[:transport]}
    )

    req = Finch.build(:post, url, headers, Jason.encode!(body), finch_opts)

    case Finch.request(req, snap.finch, receive_timeout: @timeout) do
      {:ok, %Finch.Response{status: 200, body: resp_body}} ->
        :telemetry.execute(
          [:embassy_rails, :rpc, op, :stop],
          %{
            monotonic_time: System.monotonic_time(),
            duration: System.monotonic_time() - start,
            request_bytes: request_bytes,
            response_bytes: byte_size(resp_body)
          },
          %{instance: instance, transport: snap.rpc[:transport], status: 200}
        )

        {:ok, Jason.decode!(resp_body)}

      {:ok, %Finch.Response{status: status, body: resp_body}} ->
        Logger.warning("AnyCable RPC #{path} returned #{status}: #{resp_body}")

        :telemetry.execute(
          [:embassy_rails, :rpc, op, :stop],
          %{
            monotonic_time: System.monotonic_time(),
            duration: System.monotonic_time() - start,
            request_bytes: request_bytes,
            response_bytes: byte_size(resp_body)
          },
          %{instance: instance, transport: snap.rpc[:transport], status: status}
        )

        {:error, {:http, status, resp_body}}

      {:error, reason} ->
        Logger.error("AnyCable RPC #{path} transport error: #{inspect(reason)}")

        :telemetry.execute(
          [:embassy_rails, :rpc, :exception],
          %{
            monotonic_time: System.monotonic_time(),
            duration: System.monotonic_time() - start
          },
          %{instance: instance, kind: kind_for(op), reason: reason}
        )

        {:error, reason}
    end
  end

  defp request_target(path, {:uds, socket_path}),
    do: {"http://upstream" <> path, [unix_socket: socket_path]}

  defp request_target(path, {:http, base_url}),
    do: {base_url <> path, []}

  defp kind_for(:connect), do: :connect_error
  defp kind_for(:command), do: :command_error
  defp kind_for(:disconnect), do: :disconnect_error
end
