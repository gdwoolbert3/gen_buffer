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

  defmodule State do
    @moduledoc false

    defstruct [
      :callback,
      :max_length,
      :max_size,
      :timeout,
      buffer: [],
      length: 0,
      size: 0,
      timer: nil
    ]

    @type t :: %__MODULE__{}
  end

  @type t :: GenServer.name() | pid()

  @gen_buffer_fields [:callback, :buffer_timeout, :max_length, :max_size]

  ################################
  # Public API
  ################################

  @doc """
  Starts a `GenBuffer` process linked to the current process.

  ## Options

  A GenBuffer can be started with the following parameters.

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

  ## Example

      GenBuffer.insert(:test_buffer, "foo")
      GenBuffer.insert(:test_buffer, "bar")

      # Assuming the flush callback is `IO.inspect/1`
      GenBuffer.flush(:test_buffer)
      #=> outputs ["foo", "bar"]

      GenBuffer.length(:test_buffer)
      #=> 0
  """
  @spec flush(t()) :: :ok
  def flush(buffer), do: GenServer.call(buffer, :flush)

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
  @spec init(keyword()) :: {:ok, State.t(), {:continue, :refresh}}
  def init(opts) do
    state = init_state(opts)
    {:ok, state, {:continue, :refresh}}
  end

  @doc false
  @impl GenServer
  @spec handle_call(atom() | tuple(), GenServer.from(), State.t()) ::
          {:reply, :ok, State.t()} | {:reply, :ok, State.t(), {:continue, :flush}}
  def handle_call(:dump, _from, state) do
    items = get_buffer_items(state)
    {:reply, items, state, {:continue, :refresh}}
  end

  def handle_call(:flush, _from, state) do
    {:reply, :ok, state, {:continue, :flush}}
  end

  def handle_call({:insert, item}, _from, state) do
    state = buffer_item(state, item)

    if flush?(state) do
      {:reply, :ok, state, {:continue, :flush}}
    else
      {:reply, :ok, state}
    end
  end

  def handle_call(:length, _from, state), do: {:reply, state.length, state}
  def handle_call(:size, _from, state), do: {:reply, state.size, state}

  @doc false
  @impl GenServer
  @spec handle_continue(:flush | :refresh, State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, :refresh}}
  def handle_continue(:flush, state) do
    state
    |> get_buffer_items()
    |> state.callback.()

    {:noreply, state, {:continue, :refresh}}
  end

  def handle_continue(:refresh, state) do
    state = refresh_state(state)
    {:noreply, state}
  end

  @doc false
  @impl GenServer
  @spec handle_info({:timeout, reference(), :flush}, State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, :flush}}
  def handle_info({:timeout, timer, :flush}, state) when timer == state.timer do
    {:noreply, state, {:continue, :flush}}
  end

  def handle_info(_, state), do: {:noreply, state}

  ################################
  # Private API
  ################################

  defp init_state(opts) do
    callback = Keyword.fetch!(opts, :callback)
    max_length = Keyword.get(opts, :max_length, :infinity)
    max_size = Keyword.get(opts, :max_size, :infinity)
    timeout = Keyword.get(opts, :buffer_timeout, :infinity)
    %State{callback: callback, max_length: max_length, max_size: max_size, timeout: timeout}
  end

  defp refresh_state(state) do
    cancel_upcoming_flush(state)
    timer = schedule_next_flush(state)
    %{state | buffer: [], length: 0, size: 0, timer: timer}
  end

  defp cancel_upcoming_flush(%{timer: nil}), do: :ok
  defp cancel_upcoming_flush(state), do: Process.cancel_timer(state.timer)

  defp schedule_next_flush(%{timeout: :infinity}), do: nil

  defp schedule_next_flush(state) do
    # We use `:erlang.start_timer/3` to include the timer ref in the message
    # This is necessary for handling occasional race conditions
    :erlang.start_timer(state.timeout, self(), :flush)
  end

  defp get_buffer_items(state), do: Enum.reverse(state.buffer)

  defp buffer_item(state, item) do
    size = state.size + item_size(item)
    length = state.length + 1
    buffer = [item | state.buffer]
    %{state | size: size, buffer: buffer, length: length}
  end

  defp item_size(item) when is_bitstring(item), do: byte_size(item)

  defp item_size(item) do
    item
    |> :erlang.term_to_binary()
    |> byte_size()
  end

  defp flush?(state) do
    exceeds?(state.length, state.max_length) or exceeds?(state.size, state.max_size)
  end

  defp exceeds?(_, :infinity), do: false
  defp exceeds?(num, max), do: num >= max
end
