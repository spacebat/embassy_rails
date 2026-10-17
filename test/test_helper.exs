# Redis-backed tests (tagged :redis) run only when REDIS_URL is set, and
# multi-node tests (tagged :distributed) only when epmd is running.
redis = if System.get_env("REDIS_URL"), do: [], else: [:redis]
distributed = if match?({:ok, _}, :erl_epmd.names()), do: [], else: [:distributed]
ExUnit.start(exclude: redis ++ distributed)
