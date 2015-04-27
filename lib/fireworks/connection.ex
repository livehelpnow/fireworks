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
      registered_channels: [],
      opts: opts,
      state: :disconnected
    }}
  end

  def handle_call({:connect}, _from, s) do
    {_, s} = connect(s)
    {:reply, :ok, s}
  end

  # TODO: If channels are registered when disconnected
  def handle_cast({:register_channel, mod}, s) do
    Logger.debug "Register Channel. State: #{inspect s.state}"
    case s.state do
      :connected -> 
        case mod.connect(s.connection) do
          {:ok, ch_in, ch_out} ->
            in_ref = Process.monitor ch_in.pid
            out_ref = Process.monitor ch_out.pid
            Logger.debug "Channel Registered: #{inspect {mod, in_ref, out_ref}}"
            s = %{s | channels: [{mod, in_ref, out_ref} | s.channels], registered_channels: [mod | s.registered_channels]}
          _ -> Logger.error "Error connecting channel"

        end
      _ -> s = %{s | registered_channels: [mod | s.registered_channels]}
    end
    {:noreply, s}
  end

  def handle_info({:timeout, ref, :reconnect}, s) do
    Logger.debug "Connection Reconnect"
    {_, s} = connect(s)
    {:noreply, s}
  end

  #channel went down
  def handle_info({:DOWN, ref, :process, _, _}, s) do
    {killed_channels, alive_channels} = Enum.partition(s.channels, fn({mod, in_ref, out_ref} -> ref == in_ref or ref == out_ref) end)
    Logger.error "Channels Died: #{inspect killed_channels}"
    {:noreply, %{s | channels: alive_channels}}
  end

  defp connect(%{opts: opts} = s) do
    Logger.debug "Attempting Connection"
    timeout = opts[:timoeut] || @timeout
    prefetch_count = opts[:prefetch] || @prefetch_count
    case Connection.open(opts) do
      {:ok, conn} -> 
        Process.link(conn.pid)
        Logger.debug "Connected"
        
        # Need to recycle channel mods
        connect_channels(s.registered_channels, conn)
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