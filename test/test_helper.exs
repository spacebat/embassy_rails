# Redis-backed tests (tagged :redis) run only when REDIS_URL is set.
ExUnit.start(exclude: if(System.get_env("REDIS_URL"), do: [], else: [:redis]))
