defmodule Embassy.RailsRpcValidationTest do
  use ExUnit.Case, async: true

  describe "validate_rpc!/2" do
    test "raises for the :grpc transport when grpc is unavailable" do
      assert_raise ArgumentError, ~r/:grpc/, fn ->
        Embassy.Rails.validate_rpc!([transport: {:grpc, "localhost:50051"}, secret: "s"], false)
      end
    end

    test "passes for the :grpc transport when grpc is available" do
      assert :ok ==
               Embassy.Rails.validate_rpc!(
                 [transport: {:grpc, "localhost:50051"}, secret: "s"],
                 true
               )
    end

    test "raises for the :grpc transport when :secret is missing" do
      assert_raise ArgumentError, ~r/:secret/, fn ->
        Embassy.Rails.validate_rpc!([transport: {:grpc, "localhost:50051"}], true)
      end
    end

    test "passes for uds/http regardless of grpc availability" do
      assert :ok ==
               Embassy.Rails.validate_rpc!([transport: {:uds, "/tmp/x.sock"}, secret: "s"], false)

      assert :ok ==
               Embassy.Rails.validate_rpc!([transport: {:http, "http://x"}, secret: "s"], false)
    end
  end

  describe "rpc_children/2" do
    test "returns a GrpcChannel child for the :grpc transport" do
      children =
        Embassy.Rails.rpc_children(:inst, transport: {:grpc, "localhost:50051"}, secret: "s")

      assert [{Embassy.Rails.GrpcChannel, opts}] = children
      assert opts[:instance] == :inst
      assert opts[:endpoint] == "localhost:50051"
      assert opts[:secret] == "s"
    end

    test "returns [] for uds/http" do
      assert Embassy.Rails.rpc_children(:inst, transport: {:uds, "/tmp/x.sock"}, secret: "s") ==
               []

      assert Embassy.Rails.rpc_children(:inst, transport: {:http, "http://x"}, secret: "s") == []
    end
  end
end
