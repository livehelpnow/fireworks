defmodule Fireworks.Connection do
  use GenServer
  use AMQP

  require Logger

  @timeout 5000

  def start_link(opts) do
    case GenServer.start_link(__MODULE__, opts, name: __MODULE__) do
      {:ok, pid} ->
        timeout = opts[:timeout] || @timeout
        case GenServer.call(pid, {:connect}, timeout) do
          :ok -> {:ok, pid}
          err -> {:error, err}
        end
      {_, {:already_started, pid}} -> {:ok, pid}
      err -> err
    end
  end

  def register_channel(mod) do
    GenServer.cast(__MODULE__, {:register_channel, mod})
  end

  def init(opts) do
    {:ok, %{
      connection: nil, 
      channels: [],
      opts: opts,
      state: :disconnected
    }}
  end

  def handle_call({:connect}, _from, s) do
    {_, s} = connect(s)
    {:reply, :ok, s}
  end

  def handle_cast({:register_channel, mod}, s) do
    Logger.debug "Register Channel. State: #{inspect s.state}"
    case s.state do
      :connected -> mod.connect(s.connection)
      _ -> nil
    end
    {:noreply, %{s | channels: [mod | s.channels]}}
  end

  def handle_info({:DOWN, ref, :process, _, reason}, s) when reason in [:socket_closed_unexpectedly, :heartbeat_timeout] do
    Logger.debug "Connection Closed"
    {_, s} = connect(s)
    {:noreply, s}
  end

  defp connect(%{opts: opts} = s) do
    Logger.debug "Attempting Connection"
    timeout = opts[:timoeut] || @timeout
    prefetch_count = opts[:prefetch] || @prefetch_count
    case Connection.open(opts) do
      {:ok, conn} -> 
        Process.monitor(conn.pid)
        Logger.debug "Connected"
        
        # Need to recycle channel mods
        connect_channels(s.channels, conn)
        {:ok, %{s | 
          connection: conn,
          state: :connected
        }}
      {_, error} -> 
        Logger.debug "Connection Error"
        reconnect_timer_ref = :erlang.start_timer(timeout, self, :reconnect)
        {:error, %{s | state: :disconnected}}
    end
  end

  def connect_channels(channels, conn) do
    Enum.each(channels, fn(mod) -> mod.connect(conn) end)
  end

end