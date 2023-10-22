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

  alias ExBuffer.{Mock, State}

  @type t :: GenServer.name() | pid()

  @ex_buffer_fields [:callback, :buffer_timeout, :max_length, :max_size]

  ################################
  # Public API
  ################################

  @doc """
  Starts an `ExBuffer` process linked to the current process.

  ## Options

  An ExBuffer can be started with the following options:

    * `:callback` - The function that will be invoked to handle a flush. This
      function should expect a single parameter: a list of items. (Required
      `function()`)

    * `:buffer_timeout` - The maximum time (in ms) allowed between flushes of
      the ExBuffer. Once this amount of time has passed, the ExBuffer will be
      flushed. (Optional `non_neg_integer()`, Default = `:infinity`)

    * `:max_length` - The maximum allowed length (item count) of the ExBuffer.
      Once the limit is hit, the ExBuffer will be flushed. (Optional
      `non_neg_integer()`, Default = `:infinity`)

    * `:max_size` - The maximum allowed size (in bytes) of the ExBuffer. Once
      the limit is hit (or exceeded), the ExBuffer will be flushed. For more
      information on how size is computed, see `ExBuffer.size/1`. (Optional
      `non_neg_integer()`, Default = `:infinity`)

  Additionally, an ExBuffer can also be started with any `GenServer` options.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {opts, server_opts} = Keyword.split(opts, @ex_buffer_fields)
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

      enumerable = ["foo", "bar", "baz", "foobar", "barbaz", "foobarbaz"]

      enumerable
      |> ExBuffer.chunk!(max_length: 3, max_size: 10)
      |> Enum.into([])
      #=> [["foo", "bar", "baz"], ["foobar", "barbaz"], ["foobarbaz"]]
  """
  @spec chunk!(Enumerable.t(), keyword()) :: Enumerable.t()
  defdelegate chunk!(enumerable, opts \\ []), to: Mock

  @doc """
  Dumps the contents of the given `ExBuffer` to a list, bypassing a flush
  callback and resetting the buffer.

  While this behavior may occasionally be desriable in a production environment,
  it is intended to be used primarily for testing and debugging.

  ## Example

      ExBuffer.insert(:buffer, "foo")
      ExBuffer.insert(:buffer, "bar")

      ExBuffer.dump(:buffer)
      #=> ["foo", "bar"]

      ExBuffer.length(:buffer)
      #=> 0
  """
  @spec dump(t()) :: list()
  def dump(buffer), do: GenServer.call(buffer, :dump)

  @doc """
  Flushes the given `ExBuffer`, regardless of whether or not the flush conditions
  have been met.

  While this behavior may occasionally be desriable in a production environment,
  it is intended to be used primarily for testing and debugging.

  ## Options

  An ExBuffer can be flushed with the following options:

    * `:async` - Determines whether or not the flush will be asynchronous. (Optional
      `boolean()`, Default = `true`)

  ## Example

      ExBuffer.insert(:buffer, "foo")
      ExBuffer.insert(:buffer, "bar")

      # Assuming the flush callback is `IO.inspect/1`
      ExBuffer.flush(:buffer)
      #=> outputs ["foo", "bar"]

      ExBuffer.length(:buffer)
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
  Inserts the given item into the given `ExBuffer`.

  ## Example

      ExBuffer.insert(:buffer, "foo")
      #=> :buffer items = ["foo"]

      ExBuffer.insert(:buffer, "bar")
      #=> :buffer items = ["foo", "bar"]
  """
  @spec insert(t(), term()) :: :ok
  def insert(buffer, item), do: GenServer.call(buffer, {:insert, item})

  @doc """
  Returns the length (item count) of the given `ExBuffer`.

  While this behavior may occasionally be desriable in a production environment,
  it is intended to be used primarily for testing and debugging.

  ## Example

      ExBuffer.insert(:buffer, "foo")
      ExBuffer.insert(:buffer, "bar")

      ExBuffer.length(:buffer)
      #=> 2
  """
  @spec length(t()) :: non_neg_integer()
  def length(buffer), do: GenServer.call(buffer, :length)

  @doc """
  Retuns the size (in bytes) of the given `ExBuffer`.

  Item size is computed using `Kernel.byte_size/1`. Because this function requires
  a bitstring input, non-bitstring items are first transformed into binary using
  `:erlang.term_to_binary/1`.

  While this behavior may occasionally be desriable in a production environment,
  it is intended to be used primarily for testing and debugging.

  ## Example

      ExBuffer.insert(:buffer, "foo")
      ExBuffer.insert(:buffer, "bar")

      ExBuffer.size(:buffer)
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
          {:reply, term(), State.t()}
          | {:reply, term(), State.t(), {:continue, :flush | :refresh}}
  def handle_call(:dump, _from, state) do
    {:reply, State.items(state), refresh_state(state)}
  end

  def handle_call(:async_flush, _from, state) do
    {:reply, :ok, state, {:continue, :flush}}
  end

  def handle_call(:sync_flush, _from, state) do
    do_flush(state)
    {:reply, :ok, state, {:continue, :refresh}}
  end

  def handle_call({:insert, item}, _from, state) do
    case State.insert(state, item) do
      {:flush, state} -> {:reply, :ok, state, {:continue, :flush}}
      {:cont, state} -> {:reply, :ok, state}
    end
  end

  def handle_call(:length, _from, state), do: {:reply, state.length, state}
  def handle_call(:size, _from, state), do: {:reply, state.size, state}

  @doc false
  @impl GenServer
  @spec handle_continue(term(), State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, :refresh}}
  def handle_continue(:flush, state) do
    do_flush(state)
    {:noreply, state, {:continue, :refresh}}
  end

  def handle_continue(:refresh, state), do: {:noreply, refresh_state(state)}

  @doc false
  @impl GenServer
  @spec handle_info(term(), State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, :flush}}
  def handle_info({:timeout, timer, :flush}, state) when timer == state.timer do
    {:noreply, state, {:continue, :flush}}
  end

  def handle_info(_, state), do: {:noreply, state}

  @doc false
  @impl GenServer
  @spec terminate(term(), State.t()) :: term()
  def terminate(reason, _) when reason in [:invalid_callback, :invalid_limit], do: :ok
  def terminate(_, state), do: do_flush(state)

  ################################
  # Private API
  ################################

  defp init_state(opts) do
    with {:ok, callback} <- validate_required_callback(opts) do
      max_length = Keyword.get(opts, :max_length, :infinity)
      max_size = Keyword.get(opts, :max_size, :infinity)
      timeout = Keyword.get(opts, :buffer_timeout, :infinity)
      State.new(callback, max_length, max_size, timeout)
    end
  end

  defp validate_required_callback(opts) do
    case Keyword.get(opts, :callback) do
      nil -> {:error, :invalid_callback}
      callback -> {:ok, callback}
    end
  end

  defp refresh_state(%State{timeout: :infinity} = state), do: State.refresh(state)

  defp refresh_state(state) do
    cancel_upcoming_flush(state)
    timer = schedule_next_flush(state)
    State.refresh(state, timer)
  end

  defp cancel_upcoming_flush(%State{timer: nil}), do: :ok
  defp cancel_upcoming_flush(state), do: Process.cancel_timer(state.timer)

  defp schedule_next_flush(state) do
    # We use `:erlang.start_timer/3` to include the timer ref in the message. This is necessary
    # for handling race conditions resulting from multiple simultaneous flush conditions.
    :erlang.start_timer(state.timeout, self(), :flush)
  end

  defp do_flush(state) do
    state
    |> State.items()
    |> state.callback.()
  end
end
