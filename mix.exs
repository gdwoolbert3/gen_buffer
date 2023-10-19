defmodule GenBuffer.MixProject do
  use Mix.Project

  @version "0.1.0"

  # TODO(Gordon) - description, docs, package, deps, etc.
  def project do
    [
      app: :gen_buffer,
      version: @version,
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      name: "GenBuffer"
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end
end
