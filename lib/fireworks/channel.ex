defmodule Fireworks.Channel do
  @callback config(channel :: AMQP.Channel.t()) :: String.t()
  @callback consume(payload :: map, metadata :: map) :: any

  defmacro __using__(opts) do
    quote do
      alias AMQP.Connection
      alias AMQP.Channel
      alias AMQP.Exchange
      alias AMQP.Queue
      alias AMQP.Basic
      alias AMQP.Confirm

      unquote(config(opts))
      unquote(server())
    end
  end

  defp config(opts) do
    quote do
      var!(otp_app) = unquote(opts)[:otp_app] || raise "channel expects :otp_app to be given"
      var!(task_timeout) = unquote(opts)[:task_timeout] || 60_000
      var!(config) = Application.compile_env(var!(otp_app), __MODULE__)
    end
  end

  defp server do
    quote do
      use GenServer

      @reconnect_after_ms 5_000
      @task_timeout var!(task_timeout)
      @behaviour Fireworks.Channel
      @conn_conf var!(config)
      @prefetch_count 10

      def __connection_config__(), do: @conn_conf

      require Logger

      def start_link() do
        GenServer.start_link(__MODULE__, __connection_config__(), name: __MODULE__)
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
        Process.flag(:trap_exit, true)
        send(self(), :connect)

        app_json_library = Application.get_env(:fireworks, :json_library)
        app_json_opts = Application.get_env(:fireworks, :json_opts)

        json_library = opts[:json_library] || app_json_library || nil
        json_opts = opts[:json_opts] || app_json_opts || []

        {:ok, %{
          channel: nil,
          consumer_tag: nil,
          tasks: [],
          opts: opts,
          json_library: json_library,
          json_opts: json_opts,
          status: :disconnected
        }}
      end

      def handle_info(:connect, s) do
        case Fireworks.with_conn(Fireworks.ConnPool, fn conn ->
            {:ok, chan} = Channel.open(conn)

            Process.monitor(chan.pid)
            prefetch_count = s.opts[:prefetch] || @prefetch_count
            :ok = Basic.qos(chan, prefetch_count: prefetch_count)
            queue = config(chan)
            {:ok, consumer_tag} = Basic.consume(chan, queue, self())
            {:ok, chan, consumer_tag}
          end) do
          {:ok, chan, consumer_tag} ->
            #Logger.debug "Connected Channel: #{inspect s.opts}"
            {:noreply, %{s | channel: chan, consumer_tag: consumer_tag, status: :connected}}
          {:error, :disconnected} ->
            :timer.send_after(@reconnect_after_ms, :connect)
            {:noreply, %{s | channel: nil, consumer_tag: nil, status: :disconnected}}
        end
      end

      def handle_cast({:ack, tag}, %{state: :disconnected} = s) do
        #Basic.ack channel, tag
        #The channel died, don't ack
        {:noreply, s}
      end

      def handle_cast({:ack, tag}, %{channel: channel} = s) do
        Basic.ack channel, tag
        {:noreply, s}
      end

      def handle_cast({:reject, tag, opts}, %{state: :disconnected} = s) do
        #Basic.reject channel, tag, opts
        {:noreply, s}
      end

      def handle_cast({:reject, tag, opts}, %{channel: channel} = s) do
        #Logger.debug "Channel Reject Message: #{inspect tag}"
        #Logger.debug "Options: #{inspect opts}"
        Basic.reject channel, tag, opts
        {:noreply, s}
      end

      def handle_cast({:publish, exchange, routing_key, payload, opts}, %{} = s) do
        #Logger.debug "Channel Publish Message: #{inspect payload}"
        #Logger.debug "Options: #{inspect opts}"
        Fireworks.publish exchange, routing_key, payload, opts
        {:noreply, s}
      end

      # Confirmation sent by the broker after registering this process as a consumer
      def handle_info({:basic_consume_ok, %{consumer_tag: consumer_tag}}, s) do
        #Logger.debug "Consumer Registered: #{inspect consumer_tag}"
        {:noreply, s}
      end

      # Sent by the broker when the consumer is unexpectedly cancelled (such as after a queue deletion)
      def handle_info({:basic_cancel, %{consumer_tag: consumer_tag}}, s) do
        #Logger.error "Basic Cancel Called on Channel #{inspect __MODULE__}"
        {:stop, :normal, %{s | state: :disconnected}}
      end

      # Confirmation sent by the broker to the consumer process after a Basic.cancel
      def handle_info({:basic_cancel_ok, %{consumer_tag: consumer_tag}}, s) do
        #Logger.error "Basic Cancel OK Called on Channel #{inspect __MODULE__}"
        {:stop, :normal, %{s | state: :disconnected}}
      end

      def handle_info({:basic_deliver, payload, %{delivery_tag: tag, redelivered: redelivered} = meta}, %{channel: channel} = s) do
        # Handle Message Distribution
        #Logger.debug "AMQP Delivered Payload: #{inspect payload}"
        payload =
          case s.json_library do
            nil -> payload
            _ -> payload
                 |> s.json_library.decode!(s.json_opts)
          end

        task = Task.async(fn -> consume(payload, meta) end)
        #Logger.debug "Task: #{inspect task}"

        Process.unlink(task.pid)
        timer_ref = :erlang.start_timer(@task_timeout, self(), {:task_timeout, task, tag, redelivered, payload})

        {:noreply, %{s | tasks: [{task, timer_ref, meta} | s.tasks]}}
      end

      def handle_info({task, _}, %{tasks: tasks} = s) when is_reference(task) do
        #Logger.debug "Task Finished: #{inspect task}"
        #Logger.debug "Tasks: #{inspect tasks}"
        {finished_tasks, remaining_tasks} = Enum.split_with(tasks, fn({%{ref: ref}, _, _}) -> ref == task end)
        #Logger.debug "Finished Tasks: #{inspect finished_tasks}"
        Enum.each(finished_tasks, fn({_, timer_ref, _}) ->
          :erlang.cancel_timer(timer_ref)
        end)

        {:noreply, %{s | tasks: remaining_tasks}}
      end

      def handle_info({:EXIT, pid, reason}, s) do
        #Logger.error "Exit Message From: #{inspect pid}, reason: #{inspect reason}"
        {:noreply, s}
      end

      def handle_info({:DOWN, ref, :process, pid, reason}, %{channel: %{pid: chan_pid}} = s) when pid == chan_pid do
        #Logger.debug "Channel Died for Reason: #{inspect reason}"
        send(self(), :connect)
        {:noreply, %{s | status: :disconnected}}
      end

      def handle_info({:DOWN, ref, :process, _, {:timeout, info}}, s) do
        #Logger.error "Database timeout"
        #Logger.debug "Ref: #{inspect ref}"
        {error_tasks, remaining_tasks} = Enum.split_with(s.tasks, fn({%{ref: task_ref}, timer_ref, meta}) -> task_ref == ref end)
        Enum.each(error_tasks, fn({task, timer_ref, meta}) ->
          :erlang.cancel_timer(timer_ref)
          Basic.reject s.channel, meta.delivery_tag, requeue: true
        end)
        {:noreply, %{s | tasks: remaining_tasks}}
      end

      def handle_info({:DOWN, ref, :process, _, :normal}, s) do
        {:noreply, s}
      end

      def handle_info({:DOWN, ref, :process, _, error}, s) do
        #Logger.error "Task handled error error: #{inspect error}"
        #Logger.debug "Ref: #{inspect ref}"
        {error_tasks, remaining_tasks} = Enum.split_with(s.tasks, fn({%{ref: task_ref}, timer_ref, meta}) -> task_ref == ref end)
        Enum.each(error_tasks, fn({task, timer_ref, meta}) ->
          :erlang.cancel_timer(timer_ref)
          Basic.reject s.channel, meta.delivery_tag, requeue: false
        end)
        {:noreply, %{s | tasks: remaining_tasks}}
      end

      def handle_info({:timeout, timer_ref, {:task_timeout, %{pid: task_pid, ref: task_ref}, tag, redelivered, payload}}, %{channel: channel} = s) do
        # TODO Investigate why calls to reject were killing GenServer
        #Basic.reject channel, tag, requeue: not redelivered
        Process.exit(task_pid, :task_timeout)
        {:noreply, s}
      end

    end
  end
end
