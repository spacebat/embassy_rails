defmodule Embassy.Rails.Config do
  @moduledoc """
  Per-instance configuration accessor.

  Reads a snapshot stashed in the per-instance Registry metadata by the
  `Embassy.Rails` supervisor. Lookup is O(1) and safe to call once per
  request or once per WebSocket callback.

  The `:broadcast` keyword carries `source: :http | :redis | :redisx`.
  For `:http` it also carries `:key`; for `:redis` it carries `:redis`
  (URL or opts) and optionally `:channel`; for `:redisx` it carries
  `:redis` and optionally `:stream`, `:group`, `:block_ms` and
  `:consumer`.

  The `:presence` key is `nil` when presence is disabled
  (`presence: false`), and otherwise
  `%{ttl_ms: pos_integer, api_secret: binary | nil, upstream: binary | nil}`.
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
            broadcast: keyword(),
            presence:
              %{ttl_ms: pos_integer(), api_secret: binary() | nil, upstream: binary() | nil}
              | nil,
            headers: [binary()],
            tuning: tuning()
          }

  @type tuning :: %{
          ping_interval: pos_integer() | :infinity,
          max_mailbox: pos_integer(),
          slow_client_ms: non_neg_integer()
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

  @doc """
  Return the connection tuning (`:ping_interval`, `:max_mailbox`,
  `:slow_client_ms`) for the named instance. It is stored apart from the
  snapshot, so the per-broadcast check copies three values, not the whole
  snapshot.
  """
  @spec tuning(atom()) :: tuning()
  def tuning(name) when is_atom(name) do
    case Registry.meta(Embassy.Rails.registry_name(name), :tuning) do
      {:ok, tuning} -> tuning
      :error -> raise Embassy.Rails.NotStartedError, name: name
    end
  rescue
    ArgumentError -> reraise Embassy.Rails.NotStartedError, [name: name], __STACKTRACE__
  end
end
