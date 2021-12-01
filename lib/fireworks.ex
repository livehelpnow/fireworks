defmodule Fireworks do
  use Supervisor
  use AMQP
  require Logger

  @pool_size 5
  @conn_pool_name Fireworks.ConnPool
  @pub_pool_name  Fireworks.PubPool



  # See http://elixir-lang.org/docs/stable/elixir/Application.html
  # for more information on OTP Applications
  def start(_type, _args) do
    start_link()
  end

  def start_link do
    opts = Application.get_env(:fireworks, :connection) || []
    Supervisor.start_link(__MODULE__, opts, name: Fireworks.Supervisor)
  end

  def init(opts) do

    conn_pool_opts = [
      name: {:local, @conn_pool_name},
      worker_module: Fireworks.Connection,
      size: opts[:pool_size] || @pool_size,
      strategy: :fifo,
      max_overflow: 0
    ]

     pub_pool_opts = [
      name: {:local, @pub_pool_name},
      worker_module: Fireworks.Publisher,
      size: opts[:pool_size] || @pool_size,
      max_overflow: 0
    ]
    children = [
      :poolboy.child_spec(@conn_pool_name, conn_pool_opts, [opts]),
      :poolboy.child_spec(@pub_pool_name, pub_pool_opts, @conn_pool_name),
      #worker(Fireworks.Server, [name, conn_pool_name, opts])
    ]


    #supervise children, strategy: :one_for_one
    Supervisor.init(children, strategy: :one_for_one)

  end

  def with_conn(pool_name, fun) when is_function(fun, 1) do
    case get_conn(pool_name, 0, @pool_size) do
      {:ok, conn}      -> fun.(conn)
      {:error, reason} -> {:error, reason}
    end
  end

  defp get_conn(pool_name, retry_count, max_retry_count) do
    case :poolboy.transaction(pool_name, &GenServer.call(&1, :conn)) do
      {:ok, conn}      -> {:ok, conn}
      {:error, _reason} when retry_count < max_retry_count ->
        get_conn(pool_name, retry_count + 1, max_retry_count)
      {:error, reason} -> {:error, reason}
    end
  end

  def publish(exchange, routing_key, payload, options \\ []) do
    case get_chan(@pub_pool_name, 0, @pool_size) do
      {:ok, chan}      -> Basic.publish(chan, exchange, routing_key, payload,options)
      {:error, reason} -> {:error, reason}
    end
  end

  defp get_chan(pool_name, retry_count, max_retry_count) do
    case :poolboy.transaction(pool_name, &GenServer.call(&1, :chan)) do
      {:ok, chan}      ->
        IO.puts "Got channel"
        {:ok, chan}
      {:error, reason} when retry_count < max_retry_count ->
        IO.puts "Error getting channel #{inspect reason}"
        get_chan(pool_name, retry_count + 1, max_retry_count)
      {:error, reason} ->
        IO.puts "Error getting channel Done"
        {:error, reason}
    end
  end

end
