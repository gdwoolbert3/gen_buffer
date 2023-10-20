defmodule GenBuffer do
  @moduledoc """
  A GenBuffer is a process that maintains a collection of items and flushes
  them once certain conditions have been met.

  GenBuffers can flush based on a timeout, a maximum length (item count), a
  maximum byte size, or a combination of the three. When multiple conditions are
  used, the GenBuffer will flush when the **first** condition is met.

  GenBuffers also come with a number of helpful tools for testing and debugging.
  """

  use GenServer

  alias GenBuffer.State

  @type t :: GenServer.name() | pid()

  @gen_buffer_fields [:callback, :buffer_timeout, :max_length, :max_size]

  ################################
  # Public API
  ################################

  @doc """
  Starts a `GenBuffer` process linked to the current process.

  ## Options

  A GenBuffer can be started with the following options:

    * `:callback` - The function that will be invoked to handle a flush. This
      function should expect a single parameter: a list of items. (Required
      `function()`)

    * `:buffer_timeout` - The maximum time (in ms) allowed between flushes of
      the GenBuffer. Once this amount of time has passed, the GenBuffer will be
      flushed. (Optional `non_neg_integer()`, Default = `:infinity`)

    * `:max_length` - The maximum allowed length (item count) of the GenBuffer.
      Once the limit is hit, the GenBuffer will be flushed. (Optional
      `non_neg_integer()`, Default = `:infinity`)

    * `:max_size` - The maximum allowed size (in bytes) of the GenBuffer. Once
      the limit is hit (or exceeded), the GenBuffer will be flushed. For more
      information on how size is computed, see `GenBuffer.size/1`. (Optional
      `non_neg_integer()`, Default = `:infinity`)

  Additionally, a GenBuffer can also be started with any `GenServer` options.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {opts, server_opts} = Keyword.split(opts, @gen_buffer_fields)
    GenServer.start_link(__MODULE__, opts, server_opts)
  end

  @doc """
  Lazily chunks an enumerable based on one or more GenBuffer flush conditions.

  This function currently supports length and size conditions. If multiple
  conditions are specified, a chunk is emitted once the **first** condition is
  met (just like a GenBuffer process).

  While this function is useful in it's own right, it's included primarily as
  another way to synchronously test applications that use GenBuffer.

  ## Options

  An enumerable can be chunked with the following options:

    * `:max_length` - The maximum allowed length (item count) of a chunk.
      (Optional `non_neg_integer()`, Default = `:infinity`)

    * `:max_size` - The maximum allowed size (in bytes) of a chunk. (Optional
      `non_neg_integer()`, Default = `:infinity`)

  > #### Warning {: .warning}
  >
  > Not including either of the options above is permitted but will result in a
  > single chunk being emitted. One can achieve a similar result in a more performant
  > way using `Stream.into/2`. In that same vein, including only a `:max_length`
  > condition makes this function a less performant version of `Stream.chunk_every/2`.
  > This function is optimized for chunking by either size or size **and** count. Any other
  > chunking strategy can likely be achieved in a more efficient way using other methods.

  ## Example

      enum = ["foo", "bar", "baz", "foobar", "barbaz", "foobarbaz"]

      enum
      |> GenBuffer.chunk_enum!(max_length: 3, max_size: 10)
      |> Enum.into([])
      #=> [["foo", "bar", "baz"], ["foobar", "barbaz"], ["foobarbaz"]]
  """
  @spec chunk_enum!(Enumerable.t(), keyword()) :: Enumerable.t()
  def chunk_enum!(enum, opts \\ []) do
    opts
    |> Keyword.take([:max_length, :max_size])
    |> init_state(false)
    |> case do
      {:ok, state} -> Stream.chunk_while(enum, state, &do_insert(&2, &1), &chunk_end/1)
      {:error, reason} -> raise(ArgumentError, to_message(reason))
    end
  end

  @doc """
  Dumps the contents of the given `GenBuffer` to a list, bypassing a flush
  callback and resetting the buffer.

  While this behavior may occasionally be desriable in a production environment,
  it is intended to be used primarily for testing and debugging.

  ## Example

      GenBuffer.insert(:test_buffer, "foo")
      GenBuffer.insert(:test_buffer, "bar")

      GenBuffer.dump(:test_buffer)
      #=> ["foo", "bar"]

      GenBuffer.length(:test_buffer)
      #=> 0
  """
  @spec dump(t()) :: list()
  def dump(buffer), do: GenServer.call(buffer, :dump)

  @doc """
  Flushes the given `GenBuffer`, regardless of whether or not the flush conditions
  have been met.

  While this behavior may occasionally be desriable in a production environment,
  it is intended to be used primarily for testing and debugging.

  ## Options

  A GenBuffer can be manually flushed with the following options:

    * `:async` - Determines whether or not the flush will be asynchronous. (Optional
      `boolean()`, Default = `true`)

  ## Example

      GenBuffer.insert(:test_buffer, "foo")
      GenBuffer.insert(:test_buffer, "bar")

      # Assuming the flush callback is `IO.inspect/1`
      GenBuffer.flush(:test_buffer)
      #=> outputs ["foo", "bar"]

      GenBuffer.length(:test_buffer)
      #=> 0
  """
  @spec flush(t(), keyword()) :: :ok
  def flush(buffer, opts \\ []) do
    if Keyword.get(opts, :async, true) do
      GenServer.call(buffer, :async_flush)
    else
      GenServer.call(buffer, :sync_flush)
    end
  end

  @doc """
  Inserts the given item into the given `GenBuffer`.

  ## Example

      GenBuffer.insert(:test_buffer, "foo")
      #=> :test_buffer items = ["foo"]

      GenBuffer.insert(:test_buffer, "bar")
      #=> :test_buffer items = ["foo", "bar"]
  """
  @spec insert(t(), term()) :: :ok
  def insert(buffer, item), do: GenServer.call(buffer, {:insert, item})

  @doc """
  Returns the length (item count) of the given `GenBuffer`.

  While this behavior may occasionally be desriable in a production environment,
  it is intended to be used primarily for testing and debugging.

  ## Example

      GenBuffer.insert(:test_buffer, "foo")
      GenBuffer.insert(:test_buffer, "bar")

      GenBuffer.length(:test_buffer)
      #=> 2
  """
  @spec length(t()) :: non_neg_integer()
  def length(buffer), do: GenServer.call(buffer, :length)

  @doc """
  Retuns the size (in bytes) of the given `GenBuffer`.

  Item size is computed using `Kernel.byte_size/1`. Because this function requires
  a bitstring input, non-bitstring items are first transformed into binary using
  `:erlang.term_to_binary/1`.

  While this behavior may occasionally be desriable in a production environment,
  it is intended to be used primarily for testing and debugging.

  ## Example

      GenBuffer.insert(:test_buffer, "foo")
      GenBuffer.insert(:test_buffer, "bar")

      GenBuffer.size(:test_buffer)
      #=> 6
  """
  @spec size(t()) :: non_neg_integer()
  def size(buffer), do: GenServer.call(buffer, :size)

  ################################
  # GenServer Callbacks
  ################################

  @doc false
  @impl GenServer
  @spec init(keyword()) :: {:ok, State.t()} | {:stop, :invalid_callback | :invalid_limit}
  def init(opts) do
    case init_state(opts) do
      {:ok, state} -> {:ok, refresh_state(state)}
      {:error, reason} -> {:stop, reason}
    end
  end

  @doc false
  @impl GenServer
  @spec handle_call(term(), GenServer.from(), State.t()) ::
          {:reply, term(), State.t()} | {:reply, term(), State.t(), {:continue, {:flush, list()}}}
  def handle_call(:dump, _from, state) do
    {:reply, State.items(state), refresh_state(state)}
  end

  def handle_call(:async_flush, _from, state) do
    {state, items} = flush_state(state)
    {:reply, :ok, state, {:continue, {:flush, items}}}
  end

  def handle_call(:sync_flush, _from, state) do
    {:reply, :ok, do_sync_flush(state)}
  end

  def handle_call({:insert, item}, _from, state) do
    case do_insert(state, item) do
      {:cont, items, state} -> {:reply, :ok, state, {:continue, {:flush, items}}}
      {:cont, state} -> {:reply, :ok, state}
    end
  end

  def handle_call(:length, _from, state), do: {:reply, State.length(state), state}
  def handle_call(:size, _from, state), do: {:reply, State.size(state), state}

  @doc false
  @impl GenServer
  @spec handle_continue(term(), State.t()) :: {:noreply, State.t()}
  def handle_continue({:flush, items}, state) do
    state.callback.(items)
    {:noreply, state}
  end

  @doc false
  @impl GenServer
  @spec handle_info(term(), State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, {:flush, list()}}}
  def handle_info({:timeout, timer, :flush}, state) when timer == state.timer do
    {state, items} = flush_state(state)
    {:noreply, state, {:continue, {:flush, items}}}
  end

  def handle_info(_, state), do: {:noreply, state}

  @doc false
  @impl GenServer
  @spec terminate(term(), State.t()) :: :ok | State.t()
  def terminate(reason, _) when reason in [:invalid_callback, :invalid_limit], do: :ok
  def terminate(_, state), do: do_sync_flush(state)

  ################################
  # Private API
  ################################

  defp init_state(opts, process \\ true) do
    callback = Keyword.get(opts, :callback)
    max_length = Keyword.get(opts, :max_length, :infinity)
    max_size = Keyword.get(opts, :max_size, :infinity)
    timeout = Keyword.get(opts, :buffer_timeout, :infinity)
    State.new(callback, max_length, max_size, timeout, process)
  end

  defp do_insert(state, item) do
    state = State.insert(state, item)

    if State.flush?(state) do
      {state, items} = flush_state(state)
      {:cont, items, state}
    else
      {:cont, state}
    end
  end

  defp do_sync_flush(state) do
    {state, items} = flush_state(state)
    state.callback.(items)
    state
  end

  defp flush_state(state), do: {refresh_state(state), State.items(state)}

  defp refresh_state(%State{timeout: :infinity} = state), do: State.refresh(state)

  defp refresh_state(state) do
    cancel_upcoming_flush(state)
    timer = schedule_next_flush(state)
    State.refresh(state, timer)
  end

  defp cancel_upcoming_flush(%State{timer: nil}), do: :ok
  defp cancel_upcoming_flush(state), do: Process.cancel_timer(state.timer)

  defp schedule_next_flush(state) do
    # We use `:erlang.start_timer/3` to include the timer ref in the message
    # This is necessary for handling occasional race conditions
    :erlang.start_timer(state.timeout, self(), :flush)
  end

  defp chunk_end(%State{buffer: []} = state), do: {:cont, state}
  defp chunk_end(state), do: {:cont, State.items(state), refresh_state(state)}

  defp to_message(reason), do: String.replace(to_string(reason), "_", " ")
end
