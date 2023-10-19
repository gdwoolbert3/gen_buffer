defmodule GenBuffer.MixProject do
  use Mix.Project

  @version "0.1.0"

  # TODO(Gordon) - description, docs, package, deps, etc.
  def project do
    [
      app: :gen_buffer,
      version: @version,
      elixir: "~> 1.14",
      dialyzer: dialyzer(),
      start_permanent: Mix.env() == :prod,
      name: "GenBuffer",
      docs: docs(),
      aliases: aliases(),
      preferred_cli_env: preferred_cli_env(),
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp dialyzer do
    [
      plt_file: {:no_warn, "dialyzer/dialyzer.plt"},
      plt_add_apps: [:ex_unit, :mix]
    ]
  end

  defp docs do
    [
      extras: ["README.md"],
      main: "readme",
      source_url: "https://github.com/gdwoolbert3/gen_buffer"
    ]
  end

  # Aliases are shortcuts or tasks specific to the current project.
  defp aliases do
    [
      setup: [
        "local.hex --if-missing --force",
        "local.rebar --if-missing --force",
        "deps.get"
      ],
      ci: [
        "setup",
        "compile --warnings-as-errors",
        "format --check-formatted",
        "credo --strict",
        "test",
        "dialyzer --format github",
        "sobelow --config"
      ]
    ]
  end

  # Specifies the preferred env for mix commands.
  defp preferred_cli_env do
    [
      ci: :test
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, "~> 0.30.8", only: :dev, runtime: false},
      {:credo, "~> 1.7.0", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4.1", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.13.0", only: [:dev, :test], runtime: false}
    ]
  end
end
