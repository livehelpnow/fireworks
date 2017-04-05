defmodule Fireworks.Mixfile do
  use Mix.Project

  def project do
    [app: :fireworks,
     version: "0.5.2",
     elixir: "~> 1.0",
     deps: deps(),
     description: description(),
     package: package()]
  end

  # Configuration for the OTP application
  #
  # Type `mix help compile.app` for more information
  def application do
    [applications: [:amqp, :logger, :poolboy], mod: {Fireworks, []}]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type `mix help deps` for more examples and options
  defp deps do
    [
      {:amqp, "~> 0.2.0"},
      {:poolboy, "~> 1.5.1"}
    ]
  end

  defp description do
    """
    Simple elixir work queue consumption for RabbitMQ
    """
  end

  defp package do
    [maintainers: ["Eric Witchin"],
     licenses: ["Apache 2.0"],
     links: %{"Github" => "https://github.com/livehelpnow/fireworks"}]
  end
end
