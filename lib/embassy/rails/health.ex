defmodule Embassy.Rails.Health do
  @moduledoc """
  Readiness for a Kubernetes probe: false while draining
  (`Embassy.Rails.Drain`), when the AnyCable RPC is unreachable, or, for
  the `:redis` and `:redisx` broadcast sources, until the listener is
  subscribed or consuming. Keep it out of liveness: an RPC or Redis outage
  must not restart the pods.

  For the HTTP and UDS transports, any HTTP response to a `HEAD /` counts
  as reachable. For gRPC, the channel must be established and a TCP
  connection to the endpoint must open: a cached channel alone stays up
  through an outage until a call fails on it.
  """

  alias Embassy.Rails.{Config, Drain, GrpcChannel}

  @probe_timeout 1_000

  @spec ready?(atom()) :: boolean()
  def ready?(instance) do
    not Drain.draining?(instance) and source_ok?(instance) and rpc_reachable?(instance)
  end

  @doc false
  # Set by the Redis listeners. Lives in the config Registry's meta, so a
  # Registry restart clears it, and restarts the listener that sets it.
  @spec put_source_ready(atom(), boolean()) :: :ok
  def put_source_ready(instance, ready?) do
    Registry.put_meta(Embassy.Rails.registry_name(instance), :source_ready, ready?)
  rescue
    ArgumentError -> :ok
  end

  @doc false
  @spec source_ready?(atom()) :: boolean()
  def source_ready?(instance) do
    Registry.meta(Embassy.Rails.registry_name(instance), :source_ready) == {:ok, true}
  rescue
    ArgumentError -> false
  end

  defp source_ok?(instance) do
    case Config.snapshot(instance).broadcast[:source] do
      source when source in [:redis, :redisx] -> source_ready?(instance)
      _ -> true
    end
  end

  defp rpc_reachable?(instance) do
    snap = Config.snapshot(instance)

    case snap.rpc[:transport] do
      {:grpc, endpoint} ->
        match?({:ok, _}, GrpcChannel.fetch_channel(instance)) and tcp_ok?(endpoint)

      {:http, base} ->
        http_ok?(Finch.build(:head, base), snap.finch)

      {:uds, path} ->
        http_ok?(Finch.build(:head, "http://localhost/", [], nil, unix_socket: path), snap.finch)
    end
  end

  defp tcp_ok?("unix:" <> path), do: connect_ok?({:local, path}, 0)

  defp tcp_ok?(endpoint) do
    case endpoint |> String.replace(~r{^\w+://}, "") |> String.split(":") |> Enum.split(-1) do
      {[_ | _] = host, [port]} ->
        case Integer.parse(port) do
          {port, ""} -> connect_ok?(host |> Enum.join(":") |> String.to_charlist(), port)
          _ -> true
        end

      # An endpoint this probe cannot parse is left to the channel check.
      _ ->
        true
    end
  end

  defp connect_ok?(address, port) do
    case :gen_tcp.connect(address, port, [:binary, active: false], @probe_timeout) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        true

      {:error, _} ->
        false
    end
  end

  defp http_ok?(request, finch) do
    match?(
      {:ok, %Finch.Response{}},
      Finch.request(request, finch, receive_timeout: @probe_timeout)
    )
  end
end
