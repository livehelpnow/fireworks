defmodule Fireworks.Arrangement do
  defstruct name: nil, scopes: [] 
  require Logger
  defmacro __using__(opts) do
    
    quote do
      use GenEvent
      import unquote(__MODULE__)
      @before_compile Fireworks.Arrangement
      unquote(config(opts))
      unquote(server())
    end
  end

  @doc """
  Events will be delivered in the following format
  %{
    name: String.t,
    sub_group: String.t,
    group: String.t
    payload: %{}
    __meta__: %{
      issued_at: POSIX
      caller: channel
      caller_name: friendly name
      await: true / false
    }
  }
  """
  # defmacro on(scope, payload \\ quote(do: _), do: block) do
  #   block = Macro.escape(block, unquote: true)
  #   payload   = Macro.escape(payload)
  #   quote bind_quoted: binding do
  #     Fireworks.Handler.__firework_on_definition__(__ENV__, scope)
  #     def handle_event({unquote(scope), unquote(payload)}, s) do
  #       unquote(block)
  #       {:ok, s}
  #     end
  #   end
  # end

  defmacro on(event, scope, do: block) when is_tuple(scope) do
    block = Macro.escape(block, unquote: true)
    event   = Macro.escape(event)
    quote bind_quoted: binding do
      Fireworks.Arrangement.__firework_on_definition__(__ENV__, scope)
      
      def handle_event({scope, unquote(event)}, s) do
        unquote(block)
        {:ok, s}
      end
    
    end
  end

  defmacro __before_compile__(_) do
    quote do
      def __fireworks_arrangement__ do
        %Fireworks.Arrangement{name: __MODULE__, scopes: @firework_scopes}
      end
    end
  end

  def __firework_on_definition__(env, scope) do
    mod = env.module
    Module.put_attribute(mod, :firework_scopes, scope)
  end

  defp config(opts) do
    quote do
      Module.register_attribute(__MODULE__, :firework_scopes, accumulate: true)
      var!(otp_app) = unquote(opts)[:otp_app] || raise "arrangement expects :otp_app to be given"
    end
  end

  defp server do
    quote location: :keep, unquote: false do
      def start_link do
        case Fireworks.Connection.start_link(unquote(var!(otp_app)), __MODULE__) do
          {:ok, pid} -> 
            __fireworks_arrangement__
              |> Map.get(:scopes)
              |> Enum.each(&Fireworks.Connection.bind_scope/1)
            {:ok, pid}
          {_, err} -> {:error, err}
        end
      end
    end
  end
end