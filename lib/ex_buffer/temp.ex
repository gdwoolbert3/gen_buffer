defmodule ExBuffer.Temp do
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
    :track_size,
    length: 0,
    size: 0,
    table: nil,
    timer: nil
  ]

  @opaque t :: %__MODULE__{}
  @type call_timeout :: pos_integer() | :infinity

  ################################
  # Public API
  ################################

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc false
  @spec dump(GenServer.server(), call_timeout()) :: list()
  def dump(partition, timeout) do
    GenServer.call(partition, :dump, timeout)
  end

  @doc false
  @spec flush_async(GenServer.server(), call_timeout()) :: :ok
  def flush_async(partition, timeout) do
    GenServer.call(partition, :flush_async, timeout)
  end

  @doc false
  @spec flush_sync(GenServer.server(), call_timeout()) :: :ok
  def flush_sync(partition, timeout) do
    GenServer.call(partition, :flush_sync, timeout)
  end

  @doc false
  @spec info(GenServer.server(), call_timeout()) :: map()
  def info(partition, timeout) do
    GenServer.call(partition, :info, timeout)
  end

  @doc false
  @spec insert(GenServer.server(), term(), call_timeout()) :: :ok
  def insert(partition, item, timeout) do
    GenServer.call(partition, {:insert, item}, timeout)
  end

  @doc false
  @spec insert_batch_flush_async(GenServer.server(), Enumerable.t(), call_timeout()) ::
          non_neg_integer()
  def insert_batch_flush_async(partition, items, timeout) do
    GenServer.call(partition, {:insert_batch_flush_async, items}, timeout)
  end

  @doc false
  @spec insert_batch_flush_sync(GenServer.server(), Enumerable.t(), call_timeout()) ::
          non_neg_integer()
  def insert_batch_flush_sync(partition, items, timeout) do
    GenServer.call(partition, {:insert_batch_flush_sync, items}, timeout)
  end

  ################################
  # GenServer Callbacks
  ################################

  @doc false
  @impl GenServer
  @spec init(keyword()) :: {:ok, t(), {:continue, :refresh}}
  def init(opts) do
    Process.flag(:trap_exit, true)
    state = struct!(__MODULE__, opts)
    {:ok, state, {:continue, :refresh}}
  end

  @doc false
  @impl GenServer
  @spec handle_call(term(), GenServer.from(), t()) ::
          {:reply, term(), t()} | {:reply, term(), t(), {:continue, :flush | :refresh}}
  def handle_call(:dump, _, state) do
    items = get_items(state.table)
    {:reply, items, state, {:continue, :refresh}}
  end

  def handle_call(:flush_async, _, state) do
    {:reply, :ok, state, {:continue, :flush}}
  end

  def handle_call(:flush_sync, _, state) do
    do_flush_sync(state)
    {:reply, :ok, state, {:continue, :refresh}}
  end

  def handle_call(:info, _, state) do
    info = do_info(state)
    {:ok, info, state}
  end

  def handle_call({:insert, item}, _, state) do
    state = do_insert(state, item)

    if flush?(state) do
      {:reply, :ok, state, {:continue, :flush}}
    else
      {:reply, :ok, state}
    end
  end

  def handle_call({:insert_batch_flush_async, items}, _, state) do
    {state, count} = do_insert_batch_flush_async(state, items)

    if flush?(state) do
      {:reply, count, state, {:continue, :flush}}
    else
      {:reply, count, state}
    end
  end

  def handle_call({:insert_batch_flush_sync, items}, _, state) do
    # We don't need to check if flush conditions have been met here because the
    # conditions are already checked after each item, including the last one.
    {state, count} = do_insert_batch_flush_sync(state, items)
    {:reply, count, state}
  end

  @doc false
  @impl GenServer
  @spec handle_continue(term(), t()) :: {:noreply, t()} | {:noreply, t(), {:continue, :refresh}}
  def handle_continue(:flush, state) do
    do_flush_async(state)
    {:noreply, state, {:continue, :refresh}}
  end

  def handle_continue(:refresh, state) do
    state = do_refresh(state)
    {:noreply, state}
  end

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
  def terminate(_, state), do: do_flush_sync(state)

  ################################
  # Private API
  ################################

  defp do_refresh(%__MODULE__{timeout: :infinity} = state) do
    table = create_table()
    %{state | length: 0, size: 0, table: table}
  end

  defp do_refresh(%__MODULE__{timer: nil} = state) do
    table = create_table()
    timer = schedule_next_flush(state.timeout)
    %{state | length: 0, size: 0, table: table, timer: timer}
  end

  defp do_refresh(state) do
    table = create_table()
    Process.cancel_timer(state.timer)
    timer = schedule_next_flush(state.timeout)
    %{state | length: 0, size: 0, table: table, timer: timer}
  end

  defp create_table, do: :ets.new(:items, [:ordered_set, :private])

  defp schedule_next_flush(timeout) do
    # We use `:erlang.start_timer/3` to include the timer ref in the message.
    # This is important for handling race conditions resulting from multiple
    # near-simultaneous flush conditions.
    :erlang.start_timer(timeout, self(), :flush)
  end

  defp do_insert_batch_flush_async(state, items) do
    {records, {state, count}} =
      Enum.map_reduce(items, {state, 0}, fn item, {state, count} ->
        record = {state.length, item}
        size = get_item_size(state, item)
        state = %{state | length: state.length + 1, size: state.size + size}
        {record, {state, count + 1}}
      end)

    :ets.insert(state.table, records)
    {state, count}
  end

  defp do_insert_batch_flush_sync(state, items) do
    Enum.reduce(items, {state, 0}, fn item, {state, count} ->
      state = do_insert(state, item)
      if flush?(state), do: do_flush_sync(state)
      {state, count + 1}
    end)
  end

  defp do_insert(state, item) do
    size = get_item_size(state, item)
    :ets.insert(state.table, {state.length, item})
    %{state | length: state.length + 1, size: state.size + size}
  end

  defp get_item_size(%__MODULE__{track_size: false}, _), do: 0
  defp get_item_size(state, item), do: state.size_callback.(item)

  defp do_flush_async(state) do
    {:ok, flush_task} =
      Task.start(fn ->
        receive do
          {:"ETS-TRANSFER", table, _, {flush_callback, flush_opts}} ->
            do_flush(table, flush_callback, flush_opts)
        end
      end)

    flush_opts = build_flush_opts(state)
    :ets.give_away(state.table, flush_task, {state.flush_callback, flush_opts})
  end

  defp do_flush_sync(state) do
    flush_opts = build_flush_opts(state)
    do_flush(state.table, state.flush_callback, flush_opts)
  end

  defp build_flush_opts(state) do
    [
      length: state.length,
      meta: state.flush_meta,
      partition: state.partition,
      size: maybe_get_size(state)
    ]
  end

  defp do_flush(table, flush_callback, flush_opts) do
    items = get_items(table)
    flush_callback.(items, flush_opts)
  end

  defp get_items(table) do
    table
    |> :ets.tab2list()
    |> Enum.map(fn {_, item} -> item end)
  end

  defp flush?(state) do
    exceeds?(state.length, state.max_length) or exceeds?(state.size, state.max_size)
  end

  defp exceeds?(_, :infinity), do: false
  defp exceeds?(num, max), do: num >= max

  defp do_info(state) do
    %{
      length: state.length,
      max_length: state.max_length,
      max_size: state.max_size,
      next_scheduled_flush: maybe_get_next_flush(state.timer),
      partition: state.partition,
      size: maybe_get_size(state),
      timeout: state.timeout
    }
  end

  defp maybe_get_next_flush(nil), do: nil

  defp maybe_get_next_flush(timer) do
    case Process.read_timer(timer) do
      false -> nil
      remaining -> get_next_flush(remaining)
    end
  end

  defp get_next_flush(remaining) do
    :millisecond
    |> System.os_time()
    |> DateTime.from_unix!()
    |> DateTime.add(remaining, :millisecond)
  end

  defp maybe_get_size(%__MODULE__{track_size: false}), do: nil
  defp maybe_get_size(state), do: state.size
end
