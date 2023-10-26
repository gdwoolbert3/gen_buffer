defmodule ExBuffer.Buffer.Server do
  @moduledoc false

  use GenServer

  alias ExBuffer.Buffer

  @fields [:buffer_timeout, :flush_callback, :flush_meta, :max_length, :max_size, :size_callback]

  ################################
  # Public API
  ################################

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {opts, server_opts} = Keyword.split(opts, @fields)
    GenServer.start_link(__MODULE__, opts, server_opts)
  end

  @doc false
  @spec dump(GenServer.server()) :: list()
  def dump(buffer), do: GenServer.call(buffer, :dump)

  @doc false
  @spec flush(GenServer.server(), keyword()) :: :ok
  def flush(buffer, opts \\ []) do
    if Keyword.get(opts, :async, true) do
      GenServer.call(buffer, :async_flush)
    else
      GenServer.call(buffer, :sync_flush)
    end
  end

  @doc false
  @spec insert(GenServer.server(), term()) :: :ok
  def insert(buffer, item), do: GenServer.call(buffer, {:insert, item})

  @doc false
  @spec length(GenServer.server()) :: non_neg_integer()
  def length(buffer), do: GenServer.call(buffer, :length)

  @doc false
  @spec next_flush(GenServer.server()) :: non_neg_integer() | nil
  def next_flush(buffer), do: GenServer.call(buffer, :next_flush)

  @doc false
  @spec size(GenServer.server()) :: non_neg_integer()
  def size(buffer), do: GenServer.call(buffer, :size)

  ################################
  # GenServer Callbacks
  ################################

  @doc false
  @impl GenServer
  @spec init(keyword()) :: {:ok, Buffer.t(), {:continue, :refresh}} | {:stop, ExBuffer.error()}
  def init(opts) do
    case init_buffer(opts) do
      {:ok, buffer} -> {:ok, buffer, {:continue, :refresh}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @doc false
  @impl GenServer
  @spec handle_call(term(), GenServer.from(), Buffer.t()) ::
          {:reply, term(), Buffer.t()}
          | {:reply, term(), Buffer.t(), {:continue, :flush | :refresh}}
  def handle_call(:dump, _from, buffer) do
    {:reply, Buffer.items(buffer), buffer, {:continue, :refresh}}
  end

  def handle_call(:async_flush, _from, buffer) do
    {:reply, :ok, buffer, {:continue, :flush}}
  end

  def handle_call(:sync_flush, _from, buffer) do
    do_flush(buffer)
    {:reply, :ok, buffer, {:continue, :refresh}}
  end

  def handle_call({:insert, item}, _from, buffer) do
    case Buffer.insert(buffer, item) do
      {:flush, buffer} -> {:reply, :ok, buffer, {:continue, :flush}}
      {:cont, buffer} -> {:reply, :ok, buffer}
    end
  end

  def handle_call(:length, _from, buffer), do: {:reply, buffer.length, buffer}
  def handle_call(:next_flush, _from, buffer), do: {:reply, get_next_flush(buffer), buffer}
  def handle_call(:size, _from, buffer), do: {:reply, buffer.size, buffer}

  @doc false
  @impl GenServer
  @spec handle_continue(term(), Buffer.t()) ::
          {:noreply, Buffer.t()} | {:noreply, Buffer.t(), {:continue, :refresh}}
  def handle_continue(:flush, buffer) do
    do_flush(buffer)
    {:noreply, buffer, {:continue, :refresh}}
  end

  def handle_continue(:refresh, buffer), do: {:noreply, refresh(buffer)}

  @doc false
  @impl GenServer
  @spec handle_info(term(), Buffer.t()) ::
          {:noreply, Buffer.t()} | {:noreply, Buffer.t(), {:continue, :flush}}
  def handle_info({:timeout, timer, :flush}, buffer) when timer == buffer.timer do
    {:noreply, buffer, {:continue, :flush}}
  end

  def handle_info(_, buffer), do: {:noreply, buffer}

  @doc false
  @impl GenServer
  @spec terminate(term(), Buffer.t()) :: term()
  def terminate(reason, _) when reason in [:invalid_callback, :invalid_limit], do: :ok
  def terminate(_, buffer), do: do_flush(buffer)

  ################################
  # Private API
  ################################

  defp init_buffer(opts) do
    case Keyword.get(opts, :flush_callback) do
      nil -> {:error, :invalid_callback}
      _ -> Buffer.new(opts)
    end
  end

  defp refresh(%Buffer{timeout: :infinity} = buffer), do: Buffer.refresh(buffer)

  defp refresh(buffer) do
    cancel_upcoming_flush(buffer)
    timer = schedule_next_flush(buffer)
    Buffer.refresh(buffer, timer)
  end

  defp cancel_upcoming_flush(%Buffer{timer: nil}), do: :ok
  defp cancel_upcoming_flush(buffer), do: Process.cancel_timer(buffer.timer)

  defp schedule_next_flush(buffer) do
    # We use `:erlang.start_timer/3` to include the timer ref in the message. This is necessary
    # for handling race conditions resulting from multiple simultaneous flush conditions.
    :erlang.start_timer(buffer.timeout, self(), :flush)
  end

  defp get_next_flush(%Buffer{timer: nil}), do: nil

  defp get_next_flush(buffer) do
    with false <- Process.read_timer(buffer.timer), do: nil
  end

  defp do_flush(buffer) do
    opts = [length: buffer.length, meta: buffer.flush_meta, size: buffer.size]

    buffer
    |> Buffer.items()
    |> buffer.flush_callback.(opts)
  end
end