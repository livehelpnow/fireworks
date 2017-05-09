# TODO how do we handle graceful shutdown? We need to send a Basic.cancel and wait for tasks to finish before actually terminating
defmodule Fireworks.Consumer do
  use GenServer
  alias AMQP.{Basic,Channel}
  require Logger

  @default_opts %{
    prefetch: 5,
    reconnect_after_ms: 1_000,
    setup_fn: nil, # replaced with default fn in init
    task_timeout: 60_000,
  }

  def start_link(opts, genserver_opts \\ []) do
    GenServer.start_link(__MODULE__, opts, genserver_opts)
  end

  def ack(%{consumer: consumer}=meta) do
    GenServer.call(consumer, {:ack, meta})
  end

  def reject(%{consumer: consumer}=meta, opts \\ []) do
    GenServer.call(consumer, {:reject, meta, opts})
  end

  def init(opts) do
    Process.flag(:trap_exit, true)
    send(self(), :connect)
    opts = @default_opts |> Map.put(:setup_fn, fn(_chan) -> :nothing end) |> Map.merge(opts)
    state = %{
      channel: nil,
      consumer_tag: nil,
      tasks: [],
      opts: opts,
      status: :disconnected,
    }
    {:ok, state}
  end

  def handle_call({:ack, %{channel: chan, delivery_tag: tag}}, _from, %{channel: chan} = s) do
    response = Basic.ack(chan, tag)
    {:reply, response, s}
  end
  def handle_call({:ack, _meta}, _from, s) do
    {:reply, {:error, "no longer connected to that channel"}, s}
  end

  def handle_call({:reject, %{channel: chan, delivery_tag: tag}, opts}, _from, %{channel: chan} = s) do
    response = Basic.reject(chan, tag, opts)
    {:reply, response, s}
  end
  def handle_call({:reject, _meta, _opts}, _from, s) do
    {:reply, {:error, "no longer connected to that channel"}, s}
  end

  def handle_info(:connect, s) do
    Logger.debug("attempting to connect to #{s.opts.queue}")
    case attempt_to_connect(s) do
      {:ok, chan, consumer_tag} ->
        {:noreply, %{s | channel: chan, consumer_tag: consumer_tag, status: :connected}}
      {:error, :disconnected} ->
        :timer.send_after(s.opts.reconnect_after_ms, :connect)
        {:noreply, %{s | channel: nil, consumer_tag: nil, status: :disconnected}}
    end
  end

  # Confirmation sent by the broker after registering this process as a consumer
  def handle_info({:basic_consume_ok, %{consumer_tag: _consumer_tag}}, s) do
    {:noreply, s}
  end

  # Sent by the broker when the consumer is unexpectedly cancelled (such as after a queue deletion)
  def handle_info({:basic_cancel, %{consumer_tag: _consumer_tag}}, s) do
    Logger.error("the #{s.opts.queue} consumer was cancelled by the broker (basic_cancel)")
    {:stop, :normal, %{s | state: :disconnected, channel: nil}}
  end

  # Confirmation sent by the broker to the consumer process after a Basic.cancel
  def handle_info({:basic_cancel_ok, %{consumer_tag: _consumer_tag}}, s) do
    Logger.error("the #{s.opts.queue} consumer was cancelled by the broker (basic_cancel_ok)")
    {:stop, :normal, %{s | state: :disconnected, channel: nil}}
  end

  def handle_info({:basic_deliver, payload, %{delivery_tag: tag, redelivered: redelivered} = meta}, %{channel: channel} = s) do
    meta = meta |> Map.put(:channel, channel) |> Map.put(:consumer, self())
    task = Task.async(fn -> s.opts.consume_fn.(payload, meta) end)

    Process.unlink(task.pid)
    timer_ref = :erlang.start_timer(s.opts.task_timeout, self(), {:task_timeout, task, tag, redelivered, payload})

    {:noreply, %{s | tasks: [{task, timer_ref, meta} | s.tasks]}}
  end

  def handle_info({task, _}, %{tasks: tasks} = s) when is_reference(task) do
    {finished_tasks, remaining_tasks} = Enum.partition(tasks, fn({%{ref: ref}, _, _}) -> ref == task end)
    Enum.each(finished_tasks, fn({_, timer_ref, _}) ->
      :erlang.cancel_timer(timer_ref)
    end)

    {:noreply, %{s | tasks: remaining_tasks}}
  end

  def handle_info({:EXIT, pid, reason}, s) do
    Logger.error("got EXIT from #{inspect pid} (#{inspect reason})")
    {:noreply, s}
  end

  def handle_info({:DOWN, _ref, :process, chan_pid, reason}, %{channel: %{pid: chan_pid}} = s) do
    Logger.error("channel for #{s.opts.queue} died: #{inspect reason}")
    send(self(), :connect)
    {:noreply, %{s | status: :disconnected, channel: nil}}
  end

  def handle_info({:DOWN, _ref, :process, _, :normal}, s) do
    {:noreply, s}
  end

  def handle_info({:DOWN, ref, :process, _, _error}, s) do
    {error_tasks, remaining_tasks} = Enum.partition(s.tasks, fn({%{ref: task_ref}, _timer_ref, _meta}) -> task_ref == ref end)
    Enum.each(error_tasks, fn({_task, timer_ref, meta}) ->
      :erlang.cancel_timer(timer_ref)
      # TODO make the `requeue` option configurable?
      # TODO maybe this message was already ACK or REJECTED? If so rabbit will close our channel for trying to REJECT again
      handle_call({:reject, meta, requeue: false}, :not_a_real_client, s)
    end)
    {:noreply, %{s | tasks: remaining_tasks}}
  end

  def handle_info({:timeout, _timer_ref, {:task_timeout, %{pid: task_pid}, _tag, _redelivered, _payload}}, s) do
    Process.exit(task_pid, :task_timeout)
    {:noreply, s}
  end

  defp attempt_to_connect(state) do
     Fireworks.with_conn(Fireworks.ConnPool, fn conn ->
        {:ok, chan} = Channel.open(conn)
        Process.monitor(chan.pid)
        state.opts.setup_fn.(chan)
        :ok = Basic.qos(chan, prefetch_count: state.opts.prefetch)
        {:ok, consumer_tag} = Basic.consume(chan, state.opts.queue, self())
        {:ok, chan, consumer_tag}
      end)
  end
end
