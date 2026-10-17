defmodule Embassy.Rails.Health do
  @moduledoc """
  Readiness for a Kubernetes probe: false while draining
  (`Embassy.Rails.Drain`) or when the AnyCable RPC is unreachable. Keep it
  out of liveness: an RPC outage must not restart the pods.

  For the HTTP and UDS transports, any HTTP response to a `HEAD /` counts
  as reachable; for gRPC, an established channel does.
  """

  alias Embassy.Rails.{Config, Drain, GrpcChannel}

  @probe_timeout 1_000

  @spec ready?(atom()) :: boolean()
  def ready?(instance) do
    not Drain.draining?(instance) and rpc_reachable?(instance)
  end

  defp rpc_reachable?(instance) do
    snap = Config.snapshot(instance)

    case snap.rpc[:transport] do
      {:grpc, _} ->
        match?({:ok, _}, GrpcChannel.fetch_channel(instance))

      {:http, base} ->
        http_ok?(Finch.build(:head, base), snap.finch)

      {:uds, path} ->
        http_ok?(Finch.build(:head, "http://localhost/", [], nil, unix_socket: path), snap.finch)
    end
  end

  defp http_ok?(request, finch) do
    match?(
      {:ok, %Finch.Response{}},
      Finch.request(request, finch, receive_timeout: @probe_timeout)
    )
  end
end
