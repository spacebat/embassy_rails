defmodule EmbassyRailsTest do
  use ExUnit.Case
  doctest EmbassyRails

  test "greets the world" do
    assert EmbassyRails.hello() == :world
  end
end
