defmodule ExBuffer.Partition do
  @moduledoc false

  use GenServer

  defstruct [
    :flush_callback,
    :flush_meta,
    :max_length,
    :max_size,
    :partition,
    :size_callback,
    :timeout,
    buffer: [],
    length: 0,
    size: 0,
    timer: nil
  ]

  @type error :: :invalid_jitter | :invalid_callback | :invalid_limit

  @opaque t :: %__MODULE__{}

  ################################
  # Public API
  ################################

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, opts)

  @doc false
  @spec dump(GenServer.server()) :: list()
  def dump(partition), do: GenServer.call(partition, :dump)

  @doc false
  @spec flush(GenServer.server(), keyword()) :: :ok
  def flush(partition, opts \\ []) do
    case Keyword.get(opts, :mode, :async) do
      :async -> GenServer.call(partition, :async_flush)
      :sync -> GenServer.call(partition, :sync_flush)
    end
  end

  @doc false
  @spec info(GenServer.server()) :: map()
  def info(partition), do: GenServer.call(partition, :info)

  @doc false
  @spec insert(GenServer.server(), term()) :: :ok
  def insert(partition, item), do: GenServer.call(partition, {:insert, item})

  @doc false
  @spec insert_batch(GenServer.server(), Enumerable.t(), keyword()) :: :ok
  def insert_batch(partition, items, opts \\ []) do
    case Keyword.get(opts, :flush_mode, :sync) do
      :sync -> GenServer.call(partition, {:sync_insert_batch, items})
      :async -> GenServer.call(partition, {:async_insert_batch, items})
    end
  end

  ################################
  # GenServer Callbacks
  ################################

  @doc false
  @impl GenServer
  @spec init(keyword()) :: {:ok, t(), {:continue, :refresh}} | {:stop, error()}
  def init(opts) do
    Process.flag(:trap_exit, true)

    with {:ok, jitter} <- get_jitter(opts),
         {:ok, flush_callback} <- get_flush_callback(opts),
         {:ok, size_callback} <- get_size_callback(opts),
         {:ok, max_length} <- get_max_length(opts, jitter),
         {:ok, max_size} <- get_max_size(opts, jitter),
         {:ok, timeout} <- get_timeout(opts, jitter) do
      state = %__MODULE__{
        flush_callback: flush_callback,
        flush_meta: Keyword.get(opts, :flush_meta),
        max_length: max_length,
        max_size: max_size,
        partition: Keyword.get(opts, :partition, 0),
        size_callback: size_callback,
        timeout: timeout
      }

      {:ok, state, {:continue, :refresh}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @doc false
  @impl GenServer
  @spec handle_call(term(), GenServer.from(), t()) ::
          {:reply, term(), t()} | {:reply, term(), t(), {:continue, :flush | :refresh}}
  def handle_call(:async_flush, _from, state) do
    {:reply, :ok, state, {:continue, :flush}}
  end

  def handle_call(:dump, _from, state) do
    {:reply, buffer_items(state), state, {:continue, :refresh}}
  end

  def handle_call(:info, _from, state) do
    info = %{
      length: state.length,
      max_length: state.max_length,
      max_size: state.max_size,
      next_flush: get_next_flush(state),
      partition: state.partition,
      size: state.size,
      timeout: state.timeout
    }

    {:reply, info, state}
  end

  def handle_call({:insert, item}, _from, state) do
    state = do_insert(state, item)

    if flush?(state) do
      {:reply, :ok, state, {:continue, :flush}}
    else
      {:reply, :ok, state}
    end
  end

  def handle_call({:sync_insert_batch, items}, _from, state) do
    {state, count} =
      Enum.reduce(items, {state, 0}, fn item, {state, count} ->
        state = do_insert(state, item)

        if flush?(state) do
          do_flush(state)
          {refresh_state(state), count + 1}
        else
          {state, count + 1}
        end
      end)

    {:reply, count, state}
  end

  def handle_call({:async_insert_batch, items}, _from, state) do
    {state, count} =
      Enum.reduce(items, {state, 0}, fn item, {state, count} ->
        {do_insert(state, item), count + 1}
      end)

    if flush?(state) do
      {:reply, count, state, {:continue, :flush}}
    else
      {:reply, count, state}
    end
  end

  def handle_call(:sync_flush, _from, state) do
    do_flush(state)
    {:reply, :ok, state, {:continue, :refresh}}
  end

  @doc false
  @impl GenServer
  @spec handle_continue(term(), t()) :: {:noreply, t()} | {:noreply, t(), {:continue, :refresh}}
  def handle_continue(:flush, state) do
    do_flush(state)
    {:noreply, state, {:continue, :refresh}}
  end

  def handle_continue(:refresh, state), do: {:noreply, refresh_state(state)}

  @doc false
  @impl GenServer
  @spec handle_info(term(), t()) :: {:noreply, t()} | {:noreply, t(), {:continue, :flush}}
  def handle_info({:timeout, timer, :flush}, state) when timer == state.timer do
    {:noreply, state, {:continue, :flush}}
  end

  def handle_info(_, state), do: {:noreply, state}

  @doc false
  @impl GenServer
  @spec terminate(term(), t()) :: term()
  def terminate(_, state), do: do_flush(state)

  ################################
  # Private API
  ################################

  defp get_jitter(opts) do
    case Keyword.get(opts, :jitter_rate, 0.0) do
      jitter when jitter < 0 or jitter > 1 -> {:error, :invalid_jitter}
      jitter -> {:ok, jitter}
    end
  end

  defp get_flush_callback(opts) do
    opts
    |> Keyword.get(:flush_callback)
    |> validate_callback(2)
  end

  defp get_size_callback(opts) do
    opts
    |> Keyword.get(:size_callback, &item_size/1)
    |> validate_callback(1)
  end

  defp item_size(item) when is_bitstring(item), do: byte_size(item)

  defp item_size(item) do
    item
    |> :erlang.term_to_binary()
    |> byte_size()
  end

  defp validate_callback(fun, arity) when is_function(fun, arity), do: {:ok, fun}
  defp validate_callback(_, _), do: {:error, :invalid_callback}

  defp get_max_length(opts, jitter) do
    validate_limit(Keyword.get(opts, :max_length, :infinity), jitter)
  end

  defp get_max_size(opts, jitter) do
    validate_limit(Keyword.get(opts, :max_size, :infinity), jitter)
  end

  defp get_timeout(opts, jitter) do
    validate_limit(Keyword.get(opts, :buffer_timeout, :infinity), jitter)
  end

  defp validate_limit(:infinity, _), do: {:ok, :infinity}
  defp validate_limit(_, jitter) when jitter < 0 or jitter > 1, do: {:error, :invalid_jitter}

  defp validate_limit(limit, jitter) when is_integer(limit) and limit >= 0 do
    {:ok, round(limit * (1 - jitter * :rand.uniform()))}
  end

  defp validate_limit(_, _), do: {:error, :invalid_limit}

  defp refresh_state(%__MODULE__{timeout: :infinity} = state) do
    %{state | buffer: [], length: 0, size: 0}
  end

  defp refresh_state(%__MODULE__{timer: nil} = state) do
    timer = schedule_next_flush(state)
    %{state | buffer: [], length: 0, size: 0, timer: timer}
  end

  defp refresh_state(state) do
    Process.cancel_timer(state.timer)
    timer = schedule_next_flush(state)
    %{state | buffer: [], length: 0, size: 0, timer: timer}
  end

  defp schedule_next_flush(state) do
    # We use `:erlang.start_timer/3` to include the timer ref in the message. This is used
    # for handling race conditions resulting from multiple simultaneous flush conditions.
    :erlang.start_timer(state.timeout, self(), :flush)
  end

  defp do_flush(state) do
    opts = build_flush_opts(state)

    state
    |> buffer_items()
    |> state.flush_callback.(opts)
  end

  defp build_flush_opts(state) do
    [
      length: state.length,
      meta: state.flush_meta,
      partition: state.partition,
      size: state.size
    ]
  end

  defp buffer_items(state), do: Enum.reverse(state.buffer)

  defp do_insert(state, item) do
    %{
      state
      | buffer: [item | state.buffer],
        length: state.length + 1,
        size: state.size + state.size_callback.(item)
    }
  end

  defp flush?(state) do
    exceeds?(state.length, state.max_length) or exceeds?(state.size, state.max_size)
  end

  defp exceeds?(_, :infinity), do: false
  defp exceeds?(num, max), do: num >= max

  defp get_next_flush(%__MODULE__{timer: nil}), do: nil

  defp get_next_flush(state) do
    with false <- Process.read_timer(state.timer), do: nil
  end
end
