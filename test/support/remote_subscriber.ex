defmodule Embassy.Rails.Test.RemoteSubscriber do
  @moduledoc false

  # Runs on a peer node in distributed tests. Test modules (.exs) exist only
  # on the primary node, so code spawned remotely must live in test/support.

  @doc """
  Start a `Phoenix.PubSub` on this node owned by a long-lived process. A
  PubSub started directly over `:erpc` is linked to the call's temporary
  process and dies when the call returns.
  """
  def start_pubsub(name) do
    caller = self()

    spawn(fn ->
      {:ok, _} = Phoenix.PubSub.Supervisor.start_link(name: name)
      send(caller, :pubsub_started)
      Process.sleep(:infinity)
    end)

    receive do
      :pubsub_started -> :ok
    after
      5_000 -> {:error, :timeout}
    end
  end

  @doc "Subscribe a process on this node to `topic`; it forwards one message to `test_pid`."
  def start(pubsub, topic, test_pid) do
    spawn(fn ->
      Phoenix.PubSub.subscribe(pubsub, topic)
      send(test_pid, :subscribed)

      receive do
        msg -> send(test_pid, {:remote_got, msg})
      end
    end)
  end
end
