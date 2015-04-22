defmodule Fireworks.Channel do
  defmacro __using__(opts) do
    quote do
      use GenServer
      use Behaviour

      @task_timeout 60_000
      @behaviour unquote(__MODULE__)
      #@behaviour Fireworks.Consumer

      alias AMQP.Connection
      alias AMQP.Channel
      alias AMQP.Exchange
      alias AMQP.Queue
      alias AMQP.Basic
      alias AMQP.Confirm

      require Logger

      Module.register_attribute(__MODULE__, :otp_app, accumulate: false)
      Module.put_attribute(__MODULE__, :otp_app, unquote(opts[:otp_app]))

      defcallback bind(channel :: AMQP.Channel)

      def start_link() do
        conf = Application.get_env(unquote(@otp_app), unquote(__MODULE__))
        GenServer.start_link(__MODULE__, [conf], name: __MODULE__)
      end

      def connect(opts) do
        GenServer.call(__MODULE__, {:connect, opts})
      end

      def connect_consumers(queue, opts \\ []) do
        GenServer.cast(__MODULE__, {:consume, queue, opts})
      end

      def ack(tag) do
        GenServer.cast(__MODULE__, {:ack, tag})
      end

      def reject(tag, opts \\ []) do
        GenServer.cast(__MODULE__, {:reject, tag, opts})
      end

      def publish(exchange, routing_key, payload, opts \\ []) do
        GenServer.cast(__MODULE__, {:publish, exchange, routing_key, payload, opts})
      end

      def init(opts) do
        Fireworks.Connection.register_channel(__MODULE__)
        {:ok, %{
          channel: nil,
          channel_out: nil,
          consumers: [],
          tasks: [],
          opts: opts
        }}
      end

      def handle_call({:connect, conn}, _from, s) do
        Logger.debug "Channel Open"
        {:ok, channel_in} = Channel.open(conn)
        {:ok, channel_out} = Channel.open(conn)
        config(channel_in)
        {:reply, :ok, %{s | channel: channel_in, channel_out: channel_out}}
      end

      def handle_cast({:consume, queue, opts}, s) do
        Logger.debug "Consume: #{inspect queue}"
        consumers_count = opts[:consumers] || 5
        consumers = Enum.reduce(1..consumers_count, [], fn(_, acc) ->  
          {:ok, _consumer_tag} = Basic.consume(s.channel, queue)
          [_consumer_tag | acc]
        end)
        {:noreply, %{s | consumers: consumers}}
      end

      def handle_cast({:ack, tag}, %{channel: channel} = s) do
        Basic.ack channel, tag
        {:noreply, s}
      end

      def handle_cast({:reject, tag, opts}, %{channel: channel} = s) do
        Logger.debug "Channel Reject Message: #{inspect tag}"
        Logger.debug "Options: #{inspect opts}"
        Basic.reject channel, tag, opts
        {:noreply, s}
      end

      def handle_cast({:publish, exchange, routing_key, payload, opts}, %{channel: channel, channel_out: channel_out} = s) do
        Logger.debug "Channel Publish Message: #{inspect payload}"
        Logger.debug "Options: #{inspect opts}"
        Basic.publish channel_out, exchange, routing_key, payload, opts
        {:noreply, s}
      end

      # Confirmation sent by the broker after registering this process as a consumer
      def handle_info({:basic_consume_ok, %{consumer_tag: consumer_tag}}, s) do
        Logger.debug "Consumer Registered: #{inspect consumer_tag}"
        {:noreply, s}
      end

      # Sent by the broker when the consumer is unexpectedly cancelled (such as after a queue deletion)
      def handle_info({:basic_cancel, %{consumer_tag: consumer_tag}}, s) do
        {:stop, :normal, s}
      end

      # Confirmation sent by the broker to the consumer process after a Basic.cancel
      def handle_info({:basic_cancel_ok, %{consumer_tag: consumer_tag}}, s) do
        {:noreply, s}
      end

      def handle_info({:basic_deliver, payload, %{delivery_tag: tag, redelivered: redelivered} = meta}, %{channel: channel} = s) do
        # Handle Message Distribution
        Logger.debug "AMQP Delivered Payload: #{inspect payload}"
        payload = payload
          |> Poison.decode!(keys: :atoms)

        task = Task.async(fn -> consume(payload, meta) end)
        timer_ref = :erlang.start_timer(@task_timeout, self(), {:task_timeout, task, tag, redelivered, payload})
        
        {:noreply, %{s | tasks: [{task, timer_ref} | s.tasks]}}
      end

      def handle_info({task, :ok}, %{tasks: tasks} = s) do
        Logger.debug "Task Finished: #{inspect task}"
        Logger.debug "Tasks: #{inspect tasks}"
        {finished_tasks, remaining_tasks} = Enum.partition(tasks, fn({%{pid: _, ref: ref}, _}) -> ref == task end)
        Logger.debug "Finished Tasks: #{inspect finished_tasks}"
        Enum.each(finished_tasks, fn({_, timer_ref}) -> 
          :erlang.cancel_timer(timer_ref) 
        end)
        
        {:noreply, %{s | tasks: remaining_tasks}}
      end

      def handle_info({:DOWN, ref, :process, _, :normal}, s) do
        {:noreply, s}
      end

      def handle_info({:DOWN, ref, :process, _, :task_timeout}, s) do
        {:noreply, s}
      end

      def handle_info({:timeout, timer_ref, {:task_timeout, %{pid: task_pid, ref: task_ref}, tag, redelivered, payload}}, %{channel: channel} = s) do
        #Basic.reject channel, tag, requeue: not redelivered
        Process.unlink(task_pid)
        Process.exit(task_pid, :task_timeout)
        {:noreply, s}
      end

      def handle_info({_, _}, s), do: {:noreply, s}

    end
  end
end