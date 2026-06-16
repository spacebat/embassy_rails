defmodule Embassy.Rails.Config do
  @moduledoc """
  Per-instance configuration accessor.

  Reads a snapshot stashed in the per-instance Registry metadata by the
  `Embassy.Rails` supervisor. Lookup is O(1) and safe to call once per
  request or once per WebSocket callback.

  The `:broadcast` keyword carries `source: :http | :redis`. For `:http`
  it also carries `:key`; for `:redis` it carries `:redis` (URL or opts)
  and optionally `:channel`.
  """

  @typedoc """
  Internal snapshot shape. Fields may be added without breaking
  consumers; treat the map as opaque and use the public accessors.
  """
  @opaque snapshot :: %{
            name: atom(),
            embassy: atom(),
            finch: atom(),
            pubsub: atom(),
            rpc: keyword(),
            broadcast: keyword()
          }

  @doc """
  Return the config snapshot for the named Embassy.Rails instance.

  Raises `Embassy.Rails.NotStartedError` if no Embassy.Rails supervisor
  with the given name is running.
  """
  @spec snapshot(atom()) :: snapshot()
  def snapshot(name) when is_atom(name) do
    case Registry.meta(Embassy.Rails.registry_name(name), :config) do
      {:ok, snapshot} -> snapshot
      :error -> raise Embassy.Rails.NotStartedError, name: name
    end
  rescue
    ArgumentError -> reraise Embassy.Rails.NotStartedError, [name: name], __STACKTRACE__
  end
end
