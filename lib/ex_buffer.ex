defmodule ExBuffer do
  @moduledoc """
  An `ExBuffer` is a process that maintains a collection of items and flushes
  them once certain conditions have been met.

  An `ExBuffer` can flush based on a timeout, a maximum length (item count), a
  maximum byte size, or a combination of the three. When multiple conditions are
  used, the `ExBuffer` will flush when the **first** condition is met.

  `ExBuffer` also includes a number of helpful tools for testing and debugging.
  """

  alias ExBuffer.Buffer.{Server, Stream}

  @supervisor_fields [:name, :partitioner, :partitions]

  ################################
  # Callbacks
  ################################

  @doc """
  Invoked to flush an `ExBuffer`.

  The first argument (`data`) is a list of items inserted into the `ExBuffer` and the
  second argument (`opts`) is a keyword list of flush options. See the `:flush_callback`
  and `:flush_meta` options for `ExBuffer.start_link/2` for more information.

  This callback can return any term as the return value is disregarded by the `ExBuffer`.

  This callback is required.
  """
  @callback handle_flush(data :: list(), opts :: keyword()) :: term()

  @doc """
  Invoked to determine the size of an inserted item.

  The only argument (`item`) is any term that was inserted into the `ExBuffer`.

  This callback must return a non-negative integer representing the item's byte size.

  This callback is optional. See the `:size_callback` option for `ExBuffer.start_link/2`
  for information about the default implementation.
  """
  @callback handle_size(item :: term()) :: non_neg_integer()
  @optional_callbacks handle_size: 1

  ################################
  # Types
  ################################

  @typedoc "Errors returned by `ExBuffer` functions."
  @type error :: :invalid_callback | :invalid_jitter | :invalid_limit | :invalid_partitioner

  ################################
  # Public API
  ################################

  @doc false
  @spec child_spec(keyword()) :: map()
  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc """
  Starts an `ExBuffer` process linked to the current process.

  The first argument (`module`) is optional. It is intended to be used when calling
  this function from a module that implements the `ExBuffer` behaviour. When a module
  is passed, it may interact with the options that were passed in:

  * If the module implements the `handle_flush/2` callback, it will override the
    `:flush_callback` option.

  * If the module implements the `handle_size/1` callback, it will override the
    `:size_callback` option.

  * If a `:name` option is not present, the module name will be used.

  ## Options

  An `ExBuffer` can be started with the following options:

    * `:flush_callback` - The function that will be invoked to handle a flush.
      This function should expect two parameters: a list of items and a keyword
      list of flush opts. The flush opts include the size and length of the buffer
      at the time of the flush, the partition index of the flushed buffer, and any
      provided metadata (see `:flush_meta` for more information). This function can
      return any term as the return value is not used by the `ExBuffer`. (Required)

    * `buffer_timeout` - A non-negative integer representing the maximum time
      (in ms) allowed between flushes of the `ExBuffer`. Once this amount of time
      has passed, the `ExBuffer` will be flushed. By default, an `ExBuffer` does not
      have a timeout. (Optional)

    * `:flush_meta` - A term to be included in the flush opts under the `meta` key.
      By default, this value will be `nil`. (Optional)

    * `:jitter_rate` - A float between 0 and 1 that is used to offset the limits of
      `ExBuffer` partitions. Limits are decreased by a random rate between 0 and this
      value. By default, no jitter is applied to an `ExBuffer`.

    * `:max_length` - A non-negative integer representing the maximum allowed
      length (item count) of the `ExBuffer`. Once the limit is hit, the `ExBuffer` will
      be flushed. By default, an `ExBuffer` does not have a max length. (Optional)

    * `:max_size` - A non-negative integer representing the maximum allowed size
      (in bytes) of the `ExBuffer`. Once the limit is hit (or exceeded), the `ExBuffer`
      will be flushed. The `:size_callback` option determines how item size is
      computed. By default, an `ExBuffer` does not have a max size. (Optional)

    * `:name` - The registered name for the `ExBuffer`. This must be either an atom or a
      `:via` tuple. By default (when an implementation module is not used), the name of an
      `ExBuffer` is `ExBuffer`. (Optional)

    * `:partitioner` - The strategy for assigning items to a partition. The partitioner
      can be either `:rotating` or `:random`. The former assigns items to partitions in a
      round-robin fashion and the latter assigns items randomly. By default, an `ExBuffer`
      uses a `:rotating` partition. (Optional)

    * `:partitions` - The number of partitions for the `ExBuffer`. By default, an `ExBuffer`
      has 1 partition. (Optional)

    * `:size_callback` - The function that will be invoked to determine the size
      of an item. This function should expect a single parameter representing an
      item and should return a single non-negative integer representing that item's
      byte size. The default `ExBuffer` size callback is `Kernel.byte_size/1`
      (`:erlang.term_to_binary/1` is used to convert non-bitstring inputs to binary
      if necessary). (Optional)

  Additionally, an ExBuffer can also be started with any `GenServer` options.
  """
  @spec start_link(module() | nil, keyword()) :: Supervisor.on_start()
  def start_link(module \\ nil, opts) do
    opts = maybe_update_opts(opts, module)

    with {:ok, partitions} <- validate_partitions(opts),
         {:ok, partitioner} <- validate_partitioner(opts),
         {:ok, _} = result <- do_start_link(opts) do
      partitioner = build_partitioner(partitions, partitioner)
      name = Keyword.get(opts, :name)
      put_buffer(name, partitioner, partitions)
      result
    end
  end

  @doc """
  Lazily chunks an enumerable based on `ExBuffer` flush conditions.

  This function currently supports length and size conditions. If multiple
  conditions are specified, a chunk is emitted once the **first** condition is
  met (just like an `ExBuffer` process).

  While this function is useful in it's own right, it's included primarily as
  another way to synchronously test applications that use `ExBuffer`.

  ## Options

  An enumerable can be chunked with the following options:

    * `:max_length` - A non-negative integer representing the maximum allowed
      length (item count) of a chunk. By default, there is no max length. (Optional)

    * `:max_size` - A non-negative integer representing the maximum allowed size
      (in bytes) of a chunk. The `:size_callback` option determines how item size
      is computed. By default, there is no max size. (Optional)

    * `:size_callback` - The function that will be invoked to deterime the size
      of an item. For more information, see `ExBuffer.start_link/2`. (Optional)

  > #### Warning {: .warning}
  >
  > Including neither `:max_length` nor `:max_size` is permitted but will result in a
  > single chunk being emitted. One can achieve a similar result in a more performant
  > way using `Stream.into/2`. In that same vein, including only a `:max_length`
  > condition makes this function a less performant version of `Stream.chunk_every/2`.
  > This function is optimized for chunking by either size or size **and** count. Any other
  > chunking strategy can likely be achieved in a more efficient way using other methods.

  ## Examples

      iex> enum = ["foo", "bar", "baz", "foobar", "barbaz", "foobarbaz"]
      ...> {:ok, enum} = ExBuffer.chunk(enum, max_length: 3, max_size: 10)
      ...> Enum.into(enum, [])
      [["foo", "bar", "baz"], ["foobar", "barbaz"], ["foobarbaz"]]

      iex> enum = ["foo", "bar", "baz"]
      ...> {:ok, enum} = ExBuffer.chunk(enum, max_size: 8, size_callback: &(byte_size(&1) + 1))
      ...> Enum.into(enum, [])
      [["foo", "bar"], ["baz"]]

      iex> ExBuffer.chunk(["foo", "bar", "baz"], max_length: -5)
      {:error, :invalid_limit}
  """
  @spec chunk(Enumerable.t(), keyword()) :: {:ok, Enumerable.t()} | {:error, error()}
  defdelegate chunk(enum, opts \\ []), to: Stream

  @doc """
  Lazily chunks an enumerable based on `ExBuffer` flush conditions and raises an `ArgumentError`
  with invalid options.

  For more information on this function's usage, purpose, and options, see `ExBuffer.chunk!/2`.

  ## Examples

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
  defdelegate chunk!(enum, opts \\ []), to: Stream

  @doc """
  Dumps the contents of the given `ExBuffer` to a list, bypassing a flush
  callback and resetting the buffer.

  While this functionality may occasionally be desriable in a production environment,
  it is intended to be used primarily for testing and debugging.

  ## Options

  An `ExBuffer` can be dumped with the following options:

    * `:partition` - A non-negative integer representing the specific partition index to
      dump. By default, this function dumps all partitions and concatenates the results
      together. (Optional)

  ## Examples

      iex> ExBuffer.insert(ExBuffer, "foo")
      iex> ExBuffer.insert(ExBuffer, "bar")
      iex> ExBuffer.dump(ExBuffer)
      {:ok, ["foo", "bar"]}

      iex> ExBuffer.insert(ExBuffer, "foo")
      iex> ExBuffer.insert(ExBuffer, "bar")
      iex> ExBuffer.dump(ExBuffer, partition: 0)
      {:ok, ["foo"]}
  """
  @spec dump(PartitionSupervisor.name(), keyword()) ::
          {:ok, list()} | {:error, :invalid_partition | :not_found}
  def dump(buffer, opts \\ []) do
    with {:ok, {_, parts}} <- fetch_buffer(buffer),
         {:ok, part} <- validate_partition(opts, parts) do
      fun = &Server.dump/1

      case part do
        :all -> {:ok, Enum.reduce(1..parts, [], &(&2 ++ do_part(buffer, &1 - 1, fun)))}
        part -> {:ok, do_part(buffer, part, fun)}
      end
    end
  end

  @doc """
  Flushes the given `ExBuffer`, regardless of whether or not the flush conditions
  have been met.

  While this functionality may occasionally be desriable in a production environment,
  it is intended to be used primarily for testing and debugging.

  ## Options

  An `ExBuffer` can be flushed with the following options:

    * `:async` - A boolean representing whether or not the flush will be asynchronous.
      By default, this value is `true`. (Optional)

    * `:partition` - A non-negative integer representing the specific partition index to
      flush. By default, this function flushes all partitions. (Optional)

  ## Example

      iex> ExBuffer.insert(ExBuffer, "foo")
      iex> ExBuffer.insert(ExBuffer, "bar")
      ...>
      ...> # Invokes flush callback on ["foo"] and then on ["bar"]
      iex> ExBuffer.flush(ExBuffer)
      :ok

      iex> ExBuffer.insert(ExBuffer, "foo")
      iex> ExBuffer.insert(ExBuffer, "bar")
      ...>
      ...> # Invokes flush callback on ["foo"]
      iex> ExBuffer.flush(ExBuffer, partition: 0)
      :ok
  """
  @spec flush(GenServer.server(), keyword()) :: :ok | {:error, :invalid_partition | :not_found}
  def flush(buffer, opts \\ []) do
    with {:ok, {_, parts}} <- fetch_buffer(buffer),
         {:ok, part} <- validate_partition(opts, parts) do
      fun = &Server.flush(&1, opts)

      case part do
        :all -> Enum.each(1..parts, &do_part(buffer, &1 - 1, fun))
        part -> do_part(buffer, part, fun)
      end
    end
  end

  @doc """
  Returns information about the given `ExBuffer`.

  This function returns a map per partition with the following keys:

    * `:length` - The number of items in the `ExBuffer` partition.

    * `:max_length` - The maximum length of the `ExBuffer` partition after applying the
      `:jitter_rate`.

    * `:max_size` - the maximum byte-size of the `ExBuffer` partition after applying the
      `:jitter_rate`.

    * `:next_flush` - The amount of time (in ms) until the next scheduled flush of the
      `ExBuffer` partition (or `nil` if the `ExBuffer` was started without a time limit).

    * `:partition` - The index of the `ExBuffer` partition.

    * `:size` - The byte-size of the `ExBuffer` partition.

    * `:timeout` - The maximum amount of time (in ms) allowed between flushes of the
      `ExBuffer` partition after applying the `:jitter_rate`.

  While this functionality may occasionally be desriable in a production environment,
  it is intended to be used primarily for testing and debugging.

  ## Options

  The information about an `ExBuffer` can be retrieved with the following options:

    * `:partition` - A non-negative integer representing the specific partition index to
      retrieve information for. By default, this function retrieves information for all
      partitions. (Optional)

  ## Examples

      iex> ExBuffer.insert(ExBuffer, "foo")
      iex> {:ok, [%{length: length}, %{}]} = ExBuffer.info(ExBuffer)
      iex> length
      1

      iex> ExBuffer.insert(ExBuffer, "foo")
      iex> {:ok, [%{length: length, partition: 0}]} = ExBuffer.info(ExBuffer, partition: 0)
      iex> length
      1
  """
  @spec info(GenServer.server(), keyword()) ::
          {:ok, list()} | {:error, :invalid_partition | :not_found}
  def info(buffer, opts \\ []) do
    with {:ok, {_, parts}} <- fetch_buffer(buffer),
         {:ok, part} <- validate_partition(opts, parts) do
      fun = &Server.info/1

      case part do
        :all -> {:ok, Enum.map(1..parts, &do_part(buffer, &1 - 1, fun))}
        part -> {:ok, [do_part(buffer, part, fun)]}
      end
    end
  end

  @doc """
  Inserts the given item into the given `ExBuffer` based on the partitioner that the
  given `ExBuffer` was started with.

  ## Example

      iex> ExBuffer.insert(ExBuffer, "foo")
      :ok
  """
  @spec insert(GenServer.server(), term()) :: :ok | {:error, :not_found}
  def insert(buffer, item) do
    with {:ok, {partitioner, _}} <- fetch_buffer(buffer) do
      do_part(buffer, partitioner.(), &Server.insert(&1, item))
    end
  end

  @doc """
  Inserts the given batch of items into the given `ExBuffer` based on the partitioner that
  the given `ExBuffer` was started with.

  All items in the batch are inserted into the same partition.

  ## Options

  A batch of items can be inserted into an `ExBuffer` with the following options:

    * `:safe_flush` - A boolean denoting whether or not to flush "safely". By default, this
      value is `true`, meaning that, if a flush condition is met while inserting items, the
      `ExBuffer` partition will synchronously flush before continuing to insert items. If
      this value is `false`, all items will be inserted before checking if any flush
      conditions have been met. Afterwards, if a flush condition has been met, the `ExBuffer`
      partition will be flushed asynchronously.

  ## Example

      iex> ExBuffer.insert_batch(ExBuffer, ["foo", "bar", "baz"])
      {:ok, 3}
  """
  @spec insert_batch(GenServer.server(), Enumerable.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, :not_found}
  def insert_batch(buffer, items, opts \\ []) do
    with {:ok, {partitioner, _}} <- fetch_buffer(buffer) do
      {:ok, do_part(buffer, partitioner.(), &Server.insert_batch(&1, items, opts))}
    end
  end

  @doc false
  @spec __using__(keyword()) :: Macro.t()
  defmacro __using__(_opts) do
    quote location: :keep do
      @behaviour ExBuffer

      if Module.get_attribute(__MODULE__, :doc) == nil do
        @doc """
        Returns a specification to start this ExBuffer under a supervisor.

        See `Supervisor`.
        """
      end

      def child_spec(opts) do
        %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
      end

      defoverridable(child_spec: 1)
    end
  end

  ################################
  # Private API
  ################################

  defguardp is_valid_part(part, parts) when part == :all or (part >= 0 and part < parts)

  defp maybe_update_opts(opts, nil), do: Keyword.put_new(opts, :name, __MODULE__)

  defp maybe_update_opts(opts, module) do
    opts
    |> Keyword.put_new(:name, module)
    |> maybe_update_flush_callback(module)
    |> maybe_update_size_callback(module)
  end

  defp maybe_update_flush_callback(opts, module) do
    if function_exported?(module, :handle_flush, 2) do
      Keyword.put(opts, :flush_callback, &module.handle_flush/2)
    else
      opts
    end
  end

  defp maybe_update_size_callback(opts, module) do
    if function_exported?(module, :handle_size, 1) do
      Keyword.put(opts, :size_callback, &module.handle_size/1)
    else
      opts
    end
  end

  defp validate_partitions(opts) do
    case Keyword.get(opts, :partitions, 1) do
      parts when is_integer(parts) and parts > 0 -> {:ok, parts}
      _ -> {:error, :invalid_partitions}
    end
  end

  defp validate_partitioner(opts) do
    case Keyword.get(opts, :partitioner, :rotating) do
      partitioner when partitioner in [:random, :rotating] -> {:ok, partitioner}
      _ -> {:error, :invalid_partitioner}
    end
  end

  defp validate_partition(opts, partitions) do
    case Keyword.get(opts, :partition, :all) do
      part when is_valid_part(part, partitions) -> {:ok, part}
      _ -> {:error, :invalid_partition}
    end
  end

  defp do_start_link(opts) do
    {sup_opts, buffer_opts} = Keyword.split(opts, @supervisor_fields)
    with_args = fn [opts], part -> [Keyword.put(opts, :partition, part)] end
    child_spec = {Server, buffer_opts}

    sup_opts
    |> Keyword.merge(with_arguments: with_args, child_spec: child_spec)
    |> PartitionSupervisor.start_link()
  end

  defp build_partitioner(1, _), do: fn -> 0 end

  defp build_partitioner(partitions, :random) do
    fn -> :rand.uniform(partitions) - 1 end
  end

  defp build_partitioner(partitions, :rotating) do
    atomics_ref = :atomics.new(1, [])

    fn ->
      case :atomics.add_get(atomics_ref, 1, 1) do
        part when part > partitions ->
          :atomics.put(atomics_ref, 1, 0)
          0

        part ->
          part - 1
      end
    end
  end

  defp put_buffer(buffer, partitioner, partitions) do
    buffer
    |> build_key()
    |> :persistent_term.put({partitioner, partitions})
  end

  defp fetch_buffer(buffer) do
    buffer
    |> build_key()
    |> :persistent_term.get(nil)
    |> case do
      nil -> {:error, :not_found}
      buffer -> {:ok, buffer}
    end
  end

  defp build_key(buffer), do: {__MODULE__, buffer}

  defp do_part(buffer, partition, fun) do
    buffer
    |> partition_name(partition)
    |> fun.()
  end

  defp partition_name(buffer, partition) do
    {:via, PartitionSupervisor, {buffer, partition}}
  end
end
