# Vendored AnyCable RPC proto

`anycable_rpc.proto` is vendored from the AnyCable project:
https://github.com/anycable/anycable/blob/master/protos/rpc.proto
(package `anycable`, service `RPC`).

The Elixir stubs under `lib/embassy/rails/grpc/` are committed so
consumers never need `protoc`. They are equivalent to the output of:

    mix escript.install hex protobuf 0.17.0
    protoc --elixir_out=plugins=grpc:./lib/embassy/rails/grpc \
      --proto_path=priv/protos priv/protos/anycable_rpc.proto

After bumping the vendored proto, regenerate (or hand-edit to match) and
run the gRPC integration test, which validates wire compatibility against
a real gRPC server.
