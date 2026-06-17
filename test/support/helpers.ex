defmodule Embassy.Rails.Test.Helpers do
  @moduledoc false

  def eventually(fun, tries \\ 50)
  def eventually(_fun, 0), do: false
  def eventually(fun, tries), do: fun.() || (Process.sleep(20) && eventually(fun, tries - 1))
end
