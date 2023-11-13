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

  The first argument argument (`module`) is optional. It is intended to be used when
  calling this function from a module that implements the `ExBuffer` behaviour. When
  a module is passed, it may interact with the options that were passed in:

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
      at the time of the flush and optionally include any provided metadata (see
      `:flush_meta` for more information). This function can return any term as the
      return value is not used by the `ExBuffer`. (Required)

    * `:buffer_timeout` - A non-negative integer representing the maximum time
      (in ms) allowed between flushes of the `ExBuffer`. Once this amount of time
      has passed, the `ExBuffer` will be flushed. By default, an `ExBuffer` does not
      have a timeout. (Optional)

    * `:flush_meta` - A term to be included in the flush opts under the `meta` key.
      By default, this value will be `nil`. (Optional)

    * `:max_length` - A non-negative integer representing the maximum allowed
      length (item count) of the `ExBuffer`. Once the limit is hit, the `ExBuffer` will
      be flushed. By default, an `ExBuffer` does not have a max length. (Optional)

    * `:max_size` - A non-negative integer representing the maximum allowed size
      (in bytes) of the `ExBuffer`. Once the limit is hit (or exceeded), the `ExBuffer`
      will be flushed. The `:size_callback` option determines how item size is
      computed. By default, an `ExBuffer` does not have a max size. (Optional)

    * `:size_callback` - The function that will be invoked to determine the size
      of an item. This function should expect a single parameter representing an
      item and should return a single non-negative integer representing that item's
      byte size. The default `ExBuffer` size callback is `Kernel.byte_size/1`
      (`:erlang.term_to_binary/1` is used to convert non-bitstring inputs to binary
      if necessary). (Optional)

  Additionally, an ExBuffer can also be started with any `GenServer` options.
  """
  @spec start_link(module() | nil, keyword()) :: GenServer.on_start()
  def start_link(module \\ nil, opts) do
    opts = maybe_update_opts(opts, module)

    with {:ok, partitions} <- validate_partitions(opts),
         {:ok, partitioner} <- validate_partitioner(opts),
         {:ok, _} = result <- do_start_link(partitions, opts) do
      partitioner = build_partitioner(partitions, partitioner)

      opts
      |> Keyword.get(:name)
      |> build_key()
      |> :persistent_term.put({partitioner, partitions})

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

  ## Example

      iex> ExBuffer.insert(:buffer, "foo")
      iex> ExBuffer.insert(:buffer, "bar")
      iex> ExBuffer.dump(:buffer)
      ["foo", "bar"]
  """
  @spec dump(GenServer.server(), keyword()) :: term()
  def dump(buffer, opts \\ []) do
    with {:ok, {_, partitions}} <- fetch_buffer(buffer),
         {:ok, partition} <- validate_partition(opts, partitions) do
      {:ok, do_dump(buffer, partitions, partition)}
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

  ## Example

      iex> ExBuffer.insert(:buffer, "foo")
      iex> ExBuffer.insert(:buffer, "bar")
      ...>
      ...> # Invokes callback on ["foo", "bar"]
      iex> ExBuffer.flush(:buffer)
      :ok
  """
  @spec flush(GenServer.server(), keyword()) :: term()
  def flush(buffer, opts \\ []) do
    with {:ok, {_, partitions}} <- fetch_buffer(buffer),
         {:ok, partition} <- validate_partition(opts, partitions) do
      do_flush(buffer, partitions, partition, opts)
    end
  end

  @doc """
  TODO(Gordon) - Add this
  """
  @spec info(GenServer.server(), keyword()) :: term()
  def info(buffer, opts \\ []) do
    with {:ok, {_, partitions}} <- fetch_buffer(buffer),
         {:ok, partition} <- validate_partition(opts, partitions) do
      {:ok, do_info(buffer, partitions, partition)}
    end
  end

  @doc """
  Inserts the given item into the given `ExBuffer`.

  ## Example

      iex> ExBuffer.insert(:buffer, "foo")
      :ok
  """
  @spec insert(GenServer.server(), term()) :: term()
  def insert(buffer, item) do
    # TODO(Gordon) - add support for specifying partition here
    with {:ok, {partitioner, _}} <- fetch_buffer(buffer) do
      do_insert(buffer, partitioner, item)
    end
  end

  # TODO(Gordon) - insert batch function

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

  defp do_start_link(1, opts), do: Server.start_link(opts)

  defp do_start_link(_, opts) do
    {sup_opts, buffer_opts} = Keyword.split(opts, @supervisor_fields)
    with_args = fn [opts], part -> [Keyword.put(opts, :partition, part)] end
    child_spec = {Server, buffer_opts}

    sup_opts
    |> Keyword.merge(with_arguments: with_args, child_spec: child_spec)
    |> PartitionSupervisor.start_link()
  end

  defp build_partitioner(1, _), do: :unpartitioned

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

  defp fetch_buffer(buffer) do
    buffer
    |> build_key()
    |> :persistent_term.get(nil)
    |> case do
      nil -> {:error, :not_found}
      partitioner -> {:ok, partitioner}
    end
  end

  defp build_key(buffer), do: {__MODULE__, buffer}

  defp do_dump(buffer, 1, _), do: Server.dump(buffer)

  defp do_dump(buffer, partitions, :all) do
    Enum.reduce(1..partitions, [], &(&2 ++ do_dump_partition(buffer, &1 - 1)))
  end

  defp do_dump(buffer, _, partition), do: do_dump_partition(buffer, partition)

  defp do_dump_partition(buffer, partition) do
    buffer
    |> buffer_partition_name(partition)
    |> Server.dump()
  end

  defp do_flush(buffer, 1, _, opts), do: Server.flush(buffer, opts)

  defp do_flush(buffer, partitions, :all, opts) do
    Enum.each(1..partitions, &do_flush_partition(buffer, &1 - 1, opts))
  end

  defp do_flush(buffer, _, partition, opts), do: do_flush_partition(buffer, partition, opts)

  defp do_flush_partition(buffer, partition, opts) do
    buffer
    |> buffer_partition_name(partition)
    |> Server.flush(opts)
  end

  defp do_info(buffer, 1, _), do: [Server.info(buffer)]

  defp do_info(buffer, partitions, :all) do
    Enum.map(1..partitions, &do_info_partition(buffer, &1 - 1))
  end

  defp do_info(buffer, _, partition), do: [do_info_partition(buffer, partition)]

  defp do_info_partition(buffer, partition) do
    buffer
    |> buffer_partition_name(partition)
    |> Server.info()
  end

  defp buffer_partition_name(buffer, partition) do
    {:via, PartitionSupervisor, {buffer, partition}}
  end

  defp do_insert(buffer, :unpartitioned, item), do: Server.insert(buffer, item)

  defp do_insert(buffer, partitioner, item) do
    buffer
    |> buffer_partition(partitioner.())
    |> Server.insert(item)
  end

  defp buffer_partition(buffer, partition) do
    {:via, PartitionSupervisor, {buffer, partition}}
  end
end
