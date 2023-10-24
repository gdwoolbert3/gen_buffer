defmodule ExBuffer do
  @moduledoc """
  An ExBuffer is a process that maintains a collection of items and flushes
  them once certain conditions have been met.

  ExBuffers can flush based on a timeout, a maximum length (item count), a
  maximum byte size, or a combination of the three. When multiple conditions are
  used, the ExBuffer will flush when the **first** condition is met.

  ExBuffers also come with a number of helpful tools for testing and debugging.
  """

  use GenServer

  defstruct [
    :flush_callback,
    :flush_meta,
    :max_length,
    :max_size,
    :size_callback,
    :timeout,
    buffer: [],
    length: 0,
    size: 0,
    timer: nil
  ]

  @opaque t :: %__MODULE__{}

  @server_fields [:buffer_timeout, :flush_callback, :flush_meta]
  @stream_fields [:max_length, :max_size, :size_callback]
  @flush_callback_arity 2
  @size_callback_arity 1

  ################################
  # Public API
  ################################

  @doc """
  Starts an `ExBuffer` process linked to the current process.

  ## Options

  An ExBuffer can be started with the following options:

    * `:flush_callback` - The function that will be invoked to handle a flush.
      This function should expect two parameters: a list of items and a keyword
      list of flush opts. The flush opts include the size and length of the buffer
      at the time of the flush and optionally include any provided metadata (see
      `:flush_meta` for more information). (Required)

    * `:buffer_timeout` - A non-negative integer representing the maximum time
      (in ms) allowed between flushes of the ExBuffer. Once this amount of time
      has passed, the ExBuffer will be flushed. By default, an ExBuffer does not
      have a timeout. (Optional)

    * `:flush_meta` - A term to be included in the flush opts under the `meta` key.
      By default, this value will be `nil`. (Optional)

    * `:max_length` - A non-negative integer representing the maximum allowed
      length (item count) of the ExBuffer. Once the limit is hit, the ExBuffer will
      be flushed. By default, an ExBuffer does not have a max length. (Optional)

    * `:max_size` - A non-negative integer representing the maximum allowed size
      (in bytes) of the ExBuffer. Once the limit is hit (or exceeded), the ExBuffer
      will be flushed. The `:size_callback` option determines how item size is
      computed. By default, an ExBuffer does not have a max size. (Optional)

    * `:size_callback` - The function that will be invoked to determine the size
      of an item. This function should expect a single parameter representing an
      item and should return a single non-negative integer representing that item's
      byte size. By default, an ExBuffer's size callback is `Kernel.byte_size/1`
      (`:erlang.term_to_binary/1` is used to convert non-bitstring inputs to binary
      if necessary). (Optional)

  Additionally, an ExBuffer can also be started with any `GenServer` options.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {opts, server_opts} = Keyword.split(opts, @stream_fields ++ @server_fields)
    GenServer.start_link(__MODULE__, opts, server_opts)
  end

  @doc """
  Lazily chunks an enumerable based on one or more ExBuffer flush conditions.

  This function currently supports length and size conditions. If multiple
  conditions are specified, a chunk is emitted once the **first** condition is
  met (just like an ExBuffer process).

  While this function is useful in it's own right, it's included primarily as
  another way to synchronously test applications that use ExBuffer.

  ## Options

  An enumerable can be chunked with the following options:

    * `:max_length` - A non-negative integer representing the maximum allowed
      length (item count) of a chunk. By default, there is no max length. (Optional)

    * `:max_size` - A non-negative integer representing the maximum allowed size
      (in bytes) of a chunk. The `:size_callback` option determines how item size
      is computed. By default, there is no max size. (Optional)

    * `:size_callback` - The function that will be invoked to deterime the size
      of an item. For more information, see `ExBuffer.start_link/1`. (Optional)

  > #### Warning {: .warning}
  >
  > Including neither `:max_length` nor `:max_size` is permitted but will result in a
  > single chunk being emitted. One can achieve a similar result in a more performant
  > way using `Stream.into/2`. In that same vein, including only a `:max_length`
  > condition makes this function a less performant version of `Stream.chunk_every/2`.
  > This function is optimized for chunking by either size or size **and** count. Any other
  > chunking strategy can likely be achieved in a more efficient way using other methods.

  ## Example

      iex> ["foo", "bar", "baz", "foobar", "barbaz", "foobarbaz"]
      ...> |> ExBuffer.chunk!(max_length: 3, max_size: 10)
      ...> |> Enum.into([])
      [["foo", "bar", "baz"], ["foobar", "barbaz"], ["foobarbaz"]]

      iex> ["foo", "bar", "baz"]
      ...> |> ExBuffer.chunk!(max_size: 8, size_callback: &(byte_size(&1) + 1))
      ...> |> Enum.into([])
      [["foo", "bar"], ["baz"]]

      iex> ExBuffer.chunk!(["foo", "bar", "baz"], max_length: -5)
      ** (ArgumentError) invalid limit
  """
  @spec chunk!(Enumerable.t(), keyword()) :: Enumerable.t()
  def chunk!(enum, opts \\ []) do
    case init_stream_state(opts) do
      {:ok, state} -> Stream.chunk_while(enum, state, &chunk_fun(&2, &1), &after_fun/1)
      {:error, reason} -> raise(ArgumentError, to_message(reason))
    end
  end

  @doc """
  Dumps the contents of the given `ExBuffer` to a list, bypassing a flush
  callback and resetting the buffer.

  While this functionality may occasionally be desriable in a production environment,
  it is intended to be used primarily for testing and debugging.

  ## Example

      iex> ExBuffer.insert(:buffer, "foo")
      iex> ExBuffer.insert(:buffer, "bar")
      iex> ExBuffer.dump(:buffer)
      ["foo", "bar"]
  """
  @spec dump(GenServer.server()) :: list()
  def dump(buffer), do: GenServer.call(buffer, :dump)

  @doc """
  Flushes the given `ExBuffer`, regardless of whether or not the flush conditions
  have been met.

  While this functionality may occasionally be desriable in a production environment,
  it is intended to be used primarily for testing and debugging.

  ## Options

  An ExBuffer can be flushed with the following options:

    * `:async` - A boolean representing whether or not the flush will be asynchronous.
      By default, this value is `true`. (Optional)

  ## Example

      iex> ExBuffer.insert(:buffer, "foo")
      iex> ExBuffer.insert(:buffer, "bar")
      ...>
      ...> # Invokes callback on ["foo", "bar"]
      iex> ExBuffer.flush(:buffer)
      :ok
  """
  @spec flush(GenServer.server(), keyword()) :: :ok
  def flush(buffer, opts \\ []) do
    if Keyword.get(opts, :async, true) do
      GenServer.call(buffer, :async_flush)
    else
      GenServer.call(buffer, :sync_flush)
    end
  end

  @doc """
  Inserts the given item into the given `ExBuffer`.

  ## Example

      iex> ExBuffer.insert(:buffer, "foo")
      :ok
  """
  @spec insert(GenServer.server(), term()) :: :ok
  def insert(buffer, item), do: GenServer.call(buffer, {:insert, item})

  @doc """
  Returns the length (item count) of the given `ExBuffer`.

  While this functionality may occasionally be desriable in a production environment,
  it is intended to be used primarily for testing and debugging.

  ## Example

      iex> ExBuffer.insert(:buffer, "foo")
      iex> ExBuffer.insert(:buffer, "bar")
      iex> ExBuffer.length(:buffer)
      2
  """
  @spec length(GenServer.server()) :: non_neg_integer()
  def length(buffer), do: GenServer.call(buffer, :length)

  @doc """
  Returns the time (in ms) before the next scheduled flush.

  If the given ExBuffer does not have a timeout, this function returns `nil`.

  While this functionality may occasionally be desriable in a production environment,
  it is intended to be used primarily for testing and debugging.

  ## Example

      iex> next_flush = ExBuffer.next_flush(:buffer)
      ...>
      ...> # Assuming :buffer has a timeout...
      iex> is_integer(next_flush)
      true
  """
  @spec next_flush(GenServer.server()) :: non_neg_integer() | nil
  def next_flush(buffer), do: GenServer.call(buffer, :next_flush)

  @doc """
  Retuns the size (in bytes) of the given `ExBuffer`.

  For more information on how item size is computed, see `ExBuffer.start_link/1`.

  While this functionality may occasionally be desriable in a production environment,
  it is intended to be used primarily for testing and debugging.

  ## Example

      iex> ExBuffer.insert(:buffer, "foo")
      iex> ExBuffer.insert(:buffer, "bar")
      iex> ExBuffer.size(:buffer)
      6
  """
  @spec size(GenServer.server()) :: non_neg_integer()
  def size(buffer), do: GenServer.call(buffer, :size)

  ################################
  # GenServer Callbacks
  ################################

  @doc false
  @impl GenServer
  @spec init(keyword()) ::
          {:ok, t(), {:continue, :refresh}} | {:stop, :invalid_callback | :invalid_limit}
  def init(opts) do
    case init_server_state(opts) do
      {:ok, state} -> {:ok, state, {:continue, :refresh}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @doc false
  @impl GenServer
  @spec handle_call(term(), GenServer.from(), t()) ::
          {:reply, term(), t()} | {:reply, term(), t(), {:continue, :flush | :refresh}}
  def handle_call(:dump, _from, state) do
    {:reply, items(state), state, {:continue, :refresh}}
  end

  def handle_call(:async_flush, _from, state) do
    {:reply, :ok, state, {:continue, :flush}}
  end

  def handle_call(:sync_flush, _from, state) do
    do_flush(state)
    {:reply, :ok, state, {:continue, :refresh}}
  end

  def handle_call({:insert, item}, _from, state) do
    case do_insert(state, item) do
      {:flush, state} -> {:reply, :ok, state, {:continue, :flush}}
      {:cont, state} -> {:reply, :ok, state}
    end
  end

  def handle_call(:length, _from, state), do: {:reply, state.length, state}
  def handle_call(:next_flush, _from, state), do: {:reply, get_next_flush(state), state}
  def handle_call(:size, _from, state), do: {:reply, state.size, state}

  @doc false
  @impl GenServer
  @spec handle_continue(term(), t()) :: {:noreply, t()} | {:noreply, t(), {:continue, :refresh}}
  def handle_continue(:flush, state) do
    do_flush(state)
    {:noreply, state, {:continue, :refresh}}
  end

  def handle_continue(:refresh, state), do: {:noreply, do_refresh(state)}

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
  def terminate(reason, _) when reason in [:invalid_callback, :invalid_limit], do: :ok
  def terminate(_, state), do: do_flush(state)

  ################################
  # Private Server API
  ################################

  defp init_server_state(opts) do
    case Keyword.get(opts, :flush_callback) do
      nil -> {:error, :invalid_callback}
      _ -> init_state(opts)
    end
  end

  defp get_next_flush(%__MODULE__{timer: nil}), do: nil

  defp get_next_flush(state) do
    with false <- Process.read_timer(state.timer), do: nil
  end

  defp do_flush(state) do
    opts = [length: state.length, meta: state.flush_meta, size: state.size]

    state
    |> items()
    |> state.flush_callback.(opts)
  end

  ################################
  # Private Stream API
  ################################

  defp init_stream_state(opts) do
    opts
    |> Keyword.take(@stream_fields)
    |> init_state()
  end

  defp chunk_fun(state, item) do
    with {:flush, state} <- do_insert(state, item) do
      {:cont, items(state), do_refresh(state)}
    end
  end

  defp after_fun(%__MODULE__{buffer: []} = state), do: {:cont, state}
  defp after_fun(state), do: {:cont, items(state), do_refresh(state)}

  defp to_message(reason), do: String.replace(to_string(reason), "_", " ")

  ################################
  # Private State Update API
  ################################

  defp do_refresh(%__MODULE__{timeout: :infinity} = state) do
    %{state | buffer: [], length: 0, size: 0}
  end

  defp do_refresh(state) do
    cancel_upcoming_flush(state)
    timer = schedule_next_flush(state)
    %{state | buffer: [], length: 0, size: 0, timer: timer}
  end

  defp cancel_upcoming_flush(%__MODULE__{timer: nil}), do: :ok
  defp cancel_upcoming_flush(state), do: Process.cancel_timer(state.timer)

  defp schedule_next_flush(state) do
    # We use `:erlang.start_timer/3` to include the timer ref in the message. This is necessary
    # for handling race conditions resulting from multiple simultaneous flush conditions.
    :erlang.start_timer(state.timeout, self(), :flush)
  end

  defp do_insert(state, item) do
    state = %{
      state
      | buffer: [item | state.buffer],
        length: state.length + 1,
        size: state.size + state.size_callback.(item)
    }

    if flush?(state), do: {:flush, state}, else: {:cont, state}
  end

  defp flush?(state) do
    exceeds?(state.length, state.max_length) or exceeds?(state.size, state.max_size)
  end

  defp exceeds?(_, :infinity), do: false
  defp exceeds?(num, max), do: num >= max

  defp items(state), do: Enum.reverse(state.buffer)

  ################################
  # Private State Init API
  ################################

  defp init_state(opts) do
    with {:ok, flush_callback} <- get_flush_callback(opts),
         {:ok, size_callback} <- get_size_callback(opts),
         {:ok, max_length} <- get_max_length(opts),
         {:ok, max_size} <- get_max_size(opts),
         {:ok, timeout} <- get_timeout(opts) do
      state = %__MODULE__{
        flush_callback: flush_callback,
        flush_meta: Keyword.get(opts, :flush_meta),
        max_length: max_length,
        max_size: max_size,
        size_callback: size_callback,
        timeout: timeout
      }

      {:ok, state}
    end
  end

  defp get_flush_callback(opts) do
    case Keyword.get(opts, :flush_callback) do
      nil -> {:ok, nil}
      callback -> validate_callback(callback, @flush_callback_arity)
    end
  end

  defp get_size_callback(opts) do
    opts
    |> Keyword.get(:size_callback, &item_size/1)
    |> validate_callback(@size_callback_arity)
  end

  defp get_max_length(opts), do: validate_limit(Keyword.get(opts, :max_length, :infinity))
  defp get_max_size(opts), do: validate_limit(Keyword.get(opts, :max_size, :infinity))
  defp get_timeout(opts), do: validate_limit(Keyword.get(opts, :buffer_timeout, :infinity))

  defp validate_callback(fun, arity) when is_function(fun, arity), do: {:ok, fun}
  defp validate_callback(_, _), do: {:error, :invalid_callback}

  defp validate_limit(:infinity), do: {:ok, :infinity}
  defp validate_limit(limit) when is_integer(limit) and limit >= 0, do: {:ok, limit}
  defp validate_limit(_), do: {:error, :invalid_limit}

  defp item_size(item) when is_bitstring(item), do: byte_size(item)

  defp item_size(item) do
    item
    |> :erlang.term_to_binary()
    |> byte_size()
  end
end
