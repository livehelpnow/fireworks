#Fireworks

Fireworks is a framework for providing simplicity in connecting, configuring and consuming RabbitMQ queues. It is intended to provide automatic exchange -> queue bindings and handle failovers.

## Usage

Fireworks requires information about the main connection to be defined in the application environment
```elixir
config :fireworks, :connection,
  host: "rabbitmq.local",
  username: "guest",
  password: "guest",
  heartbeat: 30
```

This information will be used at application start to establish a connection with the RabbitMQ Node.

Fireworks consumers are required to be declared through their own modules.
```elixir
config :my_app, MyApp.WorkQueue,
  consumers: 5,
  prefetch: 100
```

Each fireworks module requires the following behavior methods to be declared
```elixir
defmodule MyApp.WorkQueue do
  use Fireworks.Channel, otp_app: :my_app

  require Logger

  def config(channel) do
    exchange = "my_exchange"
    queue = "my_queue"

    Exchange.topic(channel, exchange, durable: true)

    Queue.declare(
      channel,
      error_queue,
      durable: false
    )

    Queue.bind(channel, queue, exchange, routing_key: "#")
    queue
  end

  def consume(%{} = message, %{delivery_tag: tag}) do
    Logger.debug "Process Message: #{inspect message}"
    ack tag
  end

  def consume(msg, _), do: Logger.error "Invalid Message: #{inspect msg}"
end
```

If a connection to the rabbit node is lost, fireworks will automatically attempt a reconnection to the node.

## Contributing

The easiest way to test and contribute to the fireworks library is to develop and test the features through an example app that is leveraging the Fireworks behaviour. A test suite is in the works for this framework.
