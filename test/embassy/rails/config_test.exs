defmodule Embassy.Rails.ConfigTest do
  use ExUnit.Case, async: true

  test "Embassy.Rails.NotStartedError carries the instance name in its message" do
    err = %Embassy.Rails.NotStartedError{name: MyApp.Embassy.Rails}
    assert Exception.message(err) =~ "MyApp.Embassy.Rails"
    assert Exception.message(err) =~ "not started"
  end
end
