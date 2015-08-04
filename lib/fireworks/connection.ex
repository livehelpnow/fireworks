defmodule Fireworks.Connection do
  use GenServer
  use AMQP

  require Logger

  @reconnect_after_ms 5_000

  def start_link(opts) do
    Logger.debug "Connection Opts: #{inspect opts}"
    GenServer.start_link(__MODULE__, opts)
  end

  def init([opts]) do
    Process.flag(:trap_exit, true)
    send(self, :connect)
    {:ok, %{
      conn: nil,
      opts: opts,
      status: :disconnected
    }}
  end

  def handle_call(:conn, _from, %{status: :connected, conn: conn} = status) do
    {:reply, {:ok, conn}, status}
  end

  def handle_call(:conn, _from, %{status: :disconnected} = status) do
    {:reply, {:error, :disconnected}, status}
  end

  def handle_info(:connect, s) do
    case Connection.open(s.opts) do
      {:ok, conn} ->
        Logger.debug "Connected"
        Process.monitor(conn.pid)
        {:noreply, %{s | conn: conn, status: :connected}}
      {:error, reason} ->
        :timer.send_after(@reconnect_after_ms, :connect)
        {:noreply, s}
    end
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, %{status: :connected} = state) do
    Logger.error "lost RabbitMQ connection. Attempting to reconnect..."
    :timer.send_after(@reconnect_after_ms, :connect)
    {:noreply, %{state | conn: nil, status: :disconnected}}
  end

  def terminate(_reason, %{conn: conn, status: :connected}) do
    try do
      Connection.close(conn)
    catch
      _, _ -> :ok
    end
  end
  def terminate(_reason, _state) do
    :ok
  end

end
