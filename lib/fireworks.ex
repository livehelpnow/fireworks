defmodule Fireworks do
  use Application

  # See http://elixir-lang.org/docs/stable/elixir/Application.html
  # for more information on OTP Applications
  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    opts = Application.get_env(:fireworks, :connection)
    
    children = [
      # Define workers and child supervisors to be supervised
      #worker(Fireworks.EventManager, []),
      worker(Fireworks.Connection, [opts])
    ]

    # See http://elixir-lang.org/docs/stable/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Fireworks.Supervisor]
    Supervisor.start_link(children, opts)
  end

end