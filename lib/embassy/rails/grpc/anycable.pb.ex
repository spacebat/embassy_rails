defmodule Anycable.Status do
  @moduledoc false
  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.17.0"

  field(:ERROR, 0)
  field(:SUCCESS, 1)
  field(:FAILURE, 2)
end

defmodule Anycable.Env.HeadersEntry do
  @moduledoc false
  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.17.0"

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Anycable.Env.CstateEntry do
  @moduledoc false
  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.17.0"

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Anycable.Env.IstateEntry do
  @moduledoc false
  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.17.0"

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Anycable.Env do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.17.0"

  field(:url, 1, type: :string)
  field(:headers, 2, repeated: true, type: Anycable.Env.HeadersEntry, map: true)
  field(:cstate, 3, repeated: true, type: Anycable.Env.CstateEntry, map: true)
  field(:istate, 4, repeated: true, type: Anycable.Env.IstateEntry, map: true)
end

defmodule Anycable.EnvResponse.CstateEntry do
  @moduledoc false
  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.17.0"

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Anycable.EnvResponse.IstateEntry do
  @moduledoc false
  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.17.0"

  field(:key, 1, type: :string)
  field(:value, 2, type: :string)
end

defmodule Anycable.EnvResponse do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.17.0"

  field(:cstate, 1, repeated: true, type: Anycable.EnvResponse.CstateEntry, map: true)
  field(:istate, 2, repeated: true, type: Anycable.EnvResponse.IstateEntry, map: true)
end

defmodule Anycable.ConnectionRequest do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.17.0"

  field(:env, 3, type: Anycable.Env)
end

defmodule Anycable.ConnectionResponse do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.17.0"

  field(:status, 1, type: Anycable.Status, enum: true)
  field(:identifiers, 2, type: :string)
  field(:transmissions, 3, repeated: true, type: :string)
  field(:error_msg, 4, type: :string, json_name: "errorMsg")
  field(:env, 5, type: Anycable.EnvResponse)
end

defmodule Anycable.CommandMessage do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.17.0"

  field(:command, 1, type: :string)
  field(:identifier, 2, type: :string)
  field(:connection_identifiers, 3, type: :string, json_name: "connectionIdentifiers")
  field(:data, 4, type: :string)
  field(:env, 5, type: Anycable.Env)
end

defmodule Anycable.PresenceResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.17.0"

  field(:type, 1, type: :string)
  field(:id, 2, type: :string)
  field(:info, 3, type: :string)
  field(:ttl, 4, type: :uint32)
end

defmodule Anycable.CommandResponse do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.17.0"

  field(:status, 1, type: Anycable.Status, enum: true)
  field(:disconnect, 2, type: :bool)
  field(:stop_streams, 3, type: :bool, json_name: "stopStreams")
  field(:streams, 4, repeated: true, type: :string)
  field(:transmissions, 5, repeated: true, type: :string)
  field(:error_msg, 6, type: :string, json_name: "errorMsg")
  field(:env, 7, type: Anycable.EnvResponse)
  field(:stopped_streams, 8, repeated: true, type: :string, json_name: "stoppedStreams")
  field(:presence, 9, type: Anycable.PresenceResponse)
end

defmodule Anycable.DisconnectRequest do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.17.0"

  field(:identifiers, 1, type: :string)
  field(:subscriptions, 2, repeated: true, type: :string)
  field(:env, 5, type: Anycable.Env)
end

defmodule Anycable.DisconnectResponse do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.17.0"

  field(:status, 1, type: Anycable.Status, enum: true)
  field(:error_msg, 2, type: :string, json_name: "errorMsg")
end
