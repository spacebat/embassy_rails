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

  @doc false
  # An endpoint the probe cannot parse is left to the channel check.
  @spec tcp_ok?(String.t()) :: boolean()
  def tcp_ok?(endpoint) do
    case probe_address(endpoint) do
      {:ok, {address, port}} -> connect_ok?(address, port)
      :error -> true
    end
  end

  @doc false
  # The first address of a gRPC target: `host:port`, `[v6]:port`,
  # `dns:[//authority/]host:port`, `ipv4:a:port[,...]`,
  # `ipv6:[a]:port[,...]` or `unix:path`.
  @spec probe_address(String.t()) ::
          {:ok, {charlist() | {:local, String.t()}, non_neg_integer()}} | :error
  def probe_address("unix://" <> path), do: {:ok, {{:local, path}, 0}}
  def probe_address("unix:" <> path), do: {:ok, {{:local, path}, 0}}
  def probe_address("dns:" <> rest), do: rest |> strip_authority() |> host_port()
  def probe_address("ipv4:" <> rest), do: rest |> first_address() |> host_port()
  def probe_address("ipv6:" <> rest), do: rest |> first_address() |> host_port()
  def probe_address(target), do: host_port(target)

  defp strip_authority("//" <> rest) do
    case String.split(rest, "/", parts: 2) do
      [_authority, target] -> target
      _ -> ""
    end
  end

  defp strip_authority(target), do: target

  defp first_address(list), do: list |> String.split(",", parts: 2) |> hd()

  defp host_port("[" <> rest) do
    case String.split(rest, "]:", parts: 2) do
      [host, port] -> with_port(host, port)
      _ -> :error
    end
  end

  defp host_port(target) do
    case String.split(target, ":") do
      [host, port] when host != "" -> with_port(host, port)
      _ -> :error
    end
  end

  defp with_port(host, port) do
    case Integer.parse(port) do
      {port, ""} -> {:ok, {String.to_charlist(host), port}}
      _ -> :error
    end
  end

  defp connect_ok?(address, port) do
    family = if ipv6?(address), do: [:inet6], else: []

    case :gen_tcp.connect(address, port, [:binary, active: false] ++ family, @probe_timeout) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        true

      {:error, _} ->
        false
    end
  end

  defp ipv6?(address) when is_list(address),
    do: match?({:ok, _}, :inet.parse_ipv6strict_address(address))

  defp ipv6?(_address), do: false

  defp http_ok?(request, finch) do
    match?(
      {:ok, %Finch.Response{}},
      Finch.request(request, finch, receive_timeout: @probe_timeout)
    )
  end
end
