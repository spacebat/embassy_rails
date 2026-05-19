defmodule Embassy.Rails.NotStartedError do
  @moduledoc """
  Raised when `Embassy.Rails.Config.snapshot/1` is called for an
  instance name that does not correspond to a running `Embassy.Rails`
  supervisor.

  The most common cause is a typo in the `:embassy_rails` opt of a
  plug call site, or forgetting to add `{Embassy.Rails, name: ..., ...}`
  to the consumer application's supervision tree.
  """

  defexception [:name]

  @impl true
  def message(%__MODULE__{name: name}) do
    "Embassy.Rails instance #{inspect(name)} is not started. " <>
      "Make sure {Embassy.Rails, name: #{inspect(name)}, embassy: ..., pubsub: ..., rpc: ..., broadcast: ...} " <>
      "is in your application's supervision tree."
  end
end
