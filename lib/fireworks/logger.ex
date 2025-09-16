defmodule Fireworks.Logger do
  #use GenEvent
  require Logger

  @default_format "$time $metadata[$level] $message\n"

  ### Public API
  def init({__MODULE__, opts}) do
    {:ok, configure(opts)}
  end

  def handle_call({:configure, options}, %{name: name}) do
    {:ok, :ok, configure(Keyword.put(options, :otp_name, name))}
  end

  def handle_event({_level, gl, _event}, state) when node(gl) != node() do
    {:ok, state}
  end

  def handle_event({level, _gl, {Logger, msg, ts, md}}, %{level: min_level} = state) do
    if is_nil(min_level) or Logger.compare_levels(level, min_level) != :lt do
      log_event(level, msg, ts, md, state)
    end
    {:ok, state}
  end

  defp configure(opts) do
    IO.inspect opts
    name = Keyword.get(opts, :otp_app) || raise "Must supply Fireworks.Logger otp_app"
    json_library = Keyword.get(opts, :json_library)
    env = Application.get_env(:logger, name, [])
    opts = Keyword.merge(env, opts)
    Application.put_env(:logger, name, opts)

    level    = Keyword.get(opts, :level)
    metadata = Keyword.get(opts, :metadata, [])
    format   = Keyword.get(opts, :format, @default_format) |> Logger.Formatter.compile
    exchange = Keyword.get(opts, :exchange, "")

    %{name: name, format: format, level: level, metadata: metadata, exchange: exchange, json_library: json_library}
  end

  defp log_event(level, msg, _ts, _md, state) do
    IO.puts "Loging: #{inspect to_string(level)}, #{inspect msg}"

    msg = case state.json_library do
      nil -> msg
      json_library -> %{message: msg, level: level, node: node()} |> json_library.encode!
    end

    Fireworks.publish(state.exchange, Atom.to_string(level), msg, [])
    {:ok, state}
  end

end
