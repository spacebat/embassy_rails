defmodule Embassy.Rails.RPC do
  @moduledoc """
  AnyCable RPC client. Dispatches to the transport configured per instance
  via the `:rpc` opts snapshot key.

  Transport forms:

    * `{:uds, socket_path}` — Finch dials a Unix domain socket; URL
      host is cosmetic.
    * `{:http, base_url}` — Finch dials a regular HTTP server.
    * `{:grpc, endpoint}` — gRPC channel over `"host:port"`, using the
      optional `:grpc` and `:protobuf` deps.

  Three operations:

    * `connect/2` — opens a logical AnyCable connection.
    * `command/2` — one per inbound WS frame from the client.
    * `disconnect/2` — fire-and-forget on socket close.

  Each call is bounded by the instance's `rpc: [timeout: ms]` (default
  5000).
  """

  alias Embassy.Rails.Config
  alias Embassy.Rails.RPC.Grpc

  require Logger

  @timeout 5_000

  @spec connect(atom(), map()) :: {:ok, map()} | {:error, term()}
  def connect(instance, env), do: dispatch(instance, :connect, "/_anycable/connect", %{env: env})

  @spec command(atom(), map()) :: {:ok, map()} | {:error, term()}
  def command(instance, payload), do: dispatch(instance, :command, "/_anycable/command", payload)

  @spec disconnect(atom(), map()) :: :ok
  def disconnect(instance, payload) do
    Task.start(fn -> dispatch(instance, :disconnect, "/_anycable/disconnect", payload) end)
    :ok
  end

  defp dispatch(instance, op, path, body) do
    snap = Config.snapshot(instance)

    case snap.rpc[:transport] do
      {:grpc, _endpoint} -> grpc_dispatch(instance, op, body, snap)
      _ -> post(instance, op, path, body, snap)
    end
  end

  defp grpc_dispatch(instance, op, body, snap) do
    request =
      case op do
        :connect -> Grpc.build_connect_request(body.env)
        :command -> Grpc.build_command_request(body)
        :disconnect -> Grpc.build_disconnect_request(body)
      end

    start = System.monotonic_time()
    transport = snap.rpc[:transport]

    # Byte measurements omitted: Protobuf encoding cost is required to compute them.
    :telemetry.execute(
      [:embassy_rails, :rpc, op, :start],
      %{monotonic_time: start},
      %{instance: instance, transport: transport}
    )

    case Grpc.call(instance, op, request, timeout: timeout(snap)) do
      {:ok, resp} = ok ->
        now = System.monotonic_time()

        :telemetry.execute(
          [:embassy_rails, :rpc, op, :stop],
          %{monotonic_time: now, duration: now - start},
          %{instance: instance, transport: transport, status: resp["status"]}
        )

        ok

      {:error, reason} = err ->
        Logger.warning(
          "Embassy.Rails.RPC grpc dispatch failed (instance=#{inspect(instance)}, op=#{op}): #{inspect(reason)}"
        )

        now = System.monotonic_time()

        :telemetry.execute(
          [:embassy_rails, :rpc, :exception],
          %{monotonic_time: now, duration: now - start},
          %{instance: instance, kind: kind_for(op), reason: reason}
        )

        err
    end
  end

  defp post(instance, op, path, body, snap) do
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

    case Finch.request(req, snap.finch, receive_timeout: timeout(snap)) do
      {:ok, %Finch.Response{status: 200, body: resp_body}} ->
        now = System.monotonic_time()

        :telemetry.execute(
          [:embassy_rails, :rpc, op, :stop],
          %{
            monotonic_time: now,
            duration: now - start,
            request_bytes: request_bytes,
            response_bytes: byte_size(resp_body)
          },
          %{instance: instance, transport: snap.rpc[:transport], status: 200}
        )

        {:ok, Jason.decode!(resp_body)}

      {:ok, %Finch.Response{status: status, body: resp_body}} ->
        Logger.warning("AnyCable RPC #{path} returned #{status}: #{resp_body}")

        now = System.monotonic_time()

        :telemetry.execute(
          [:embassy_rails, :rpc, op, :stop],
          %{
            monotonic_time: now,
            duration: now - start,
            request_bytes: request_bytes,
            response_bytes: byte_size(resp_body)
          },
          %{instance: instance, transport: snap.rpc[:transport], status: status}
        )

        {:error, {:http, status, resp_body}}

      {:error, reason} ->
        Logger.error("AnyCable RPC #{path} transport error: #{inspect(reason)}")

        now = System.monotonic_time()

        :telemetry.execute(
          [:embassy_rails, :rpc, :exception],
          %{
            monotonic_time: now,
            duration: now - start
          },
          %{instance: instance, kind: kind_for(op), reason: reason}
        )

        {:error, reason}
    end
  end

  defp timeout(snap), do: snap.rpc[:timeout] || @timeout

  defp request_target(path, {:uds, socket_path}),
    do: {"http://upstream" <> path, [unix_socket: socket_path]}

  defp request_target(path, {:http, base_url}),
    do: {base_url <> path, []}

  defp kind_for(:connect), do: :connect_error
  defp kind_for(:command), do: :command_error
  defp kind_for(:disconnect), do: :disconnect_error
end
