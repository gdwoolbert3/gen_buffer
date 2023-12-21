defmodule ExBuffer do
  @moduledoc """
  An `ExBuffer` is a process that maintains a collection of items and flushes
  them once certain conditions have been met.

  An `ExBuffer` can flush based on a timeout, a maximum length (item count), a
  maximum byte size, or a combination of the three. When multiple conditions are
  used, the `ExBuffer` will flush when the **first** condition is met.

  `ExBuffer` also includes a number of helpful tools for testing and debugging.
  """

  alias ExBuffer.Partition

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
      return any term as the return value is disregarded by the `ExBuffer`. (Required)

    * `:buffer_timeout` - A non-negative integer representing the maximum time
      (in ms) allowed between flushes of the `ExBuffer`. Once this amount of time
      has passed, the `ExBuffer` will be flushed. By default, an `ExBuffer` does not
      have a timeout. (Optional)

    * `:flush_meta` - A term to be included in the flush opts under the `meta` key.
      By default, this value will be `nil`. (Optional)

    * `:jitter_rate` - A float between 0 and 1 that is used to offset the limits of
      `ExBuffer` partitions. Limits are **decreased** by a random rate between 0 and this
      value. By default, no jitter is applied to an `ExBuffer`. (Optional)

    * `:max_length` - A non-negative integer representing the maximum allowed
      length (item count) of the `ExBuffer`. Once the limit is hit, the `ExBuffer` will
      be flushed. By default, an `ExBuffer` does not have a max length. (Optional)

    * `:max_size` - A non-negative integer representing the maximum allowed size
      (in bytes) of the `ExBuffer`. Once the limit is hit (or exceeded), the `ExBuffer`
      will be flushed. The `:size_callback` option determines how item size is computed.
      By default, an `ExBuffer` does not have a max size. (Optional)

    * `:name` - The registered name for the `ExBuffer`. This must be either an atom or a
      `:via` tuple. By default (when an implementation module is not used), the name of an
      `ExBuffer` is `ExBuffer`. (Optional)

    * `:partitioner` - The strategy for assigning items to a partition. The partitioner
      can be either `:rotating` or `:random`. The former assigns items to partitions in a
      round-robin fashion and the latter assigns items randomly. By default, an `ExBuffer`
      uses a `:rotating` partitioner. (Optional)

    * `:partitions` - The number of partitions for the `ExBuffer`. By default, an `ExBuffer`
      has 1 partition. (Optional)

    * `:size_callback` - The function that will be invoked to determine the size of an item.
      This function should expect a single parameter representing an item and should return
      a single non-negative integer representing that item's byte size. The default
      `ExBuffer` size callback is `Kernel.byte_size/1` (`:erlang.term_to_binary/1` is used
      to convert non-bitstring inputs to binary if necessary). (Optional)

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
      ["foo", "bar"]

      iex> ExBuffer.insert(ExBuffer, "foo")
      iex> ExBuffer.insert(ExBuffer, "bar")
      iex> ExBuffer.dump(ExBuffer, partition: 0)
      ["foo"]
  """
  @spec dump(PartitionSupervisor.name(), keyword()) :: list()
  def dump(buffer, opts \\ []) do
    with {:ok, {_, parts}} <- fetch_buffer(buffer),
         {:ok, part} <- validate_partition(opts, parts) do
      Enum.reduce(enumerate_parts(parts, part), [], fn part, acc ->
        acc ++ do_part(buffer, part, &Partition.dump/1)
      end)
    else
      {:error, reason} -> raise(ArgumentError, to_message(reason))
    end
  end

  @doc """
  Flushes the given `ExBuffer`, regardless of whether or not the flush conditions
  have been met.

  While this functionality may occasionally be desriable in a production environment,
  it is intended to be used primarily for testing and debugging.

  ## Options

  An `ExBuffer` can be flushed with the following options:

    * `:mode` - A value denoting whether the flush will be synchronous or asynchronous.
      Possible values are ':sync` and `:async`. By default, this value is `:async`.
      (Optional)

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
  @spec flush(GenServer.server(), keyword()) :: :ok
  def flush(buffer, opts \\ []) do
    with {:ok, {_, parts}} <- fetch_buffer(buffer),
         {:ok, part} <- validate_partition(opts, parts) do
      Enum.each(enumerate_parts(parts, part), fn part ->
        do_part(buffer, part, &Partition.flush(&1, opts))
      end)
    else
      {:error, reason} -> raise(ArgumentError, to_message(reason))
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
      iex> [%{length: length}, %{}] = ExBuffer.info(ExBuffer)
      iex> length
      1

      iex> ExBuffer.insert(ExBuffer, "foo")
      iex> [%{length: length, partition: 0}] = ExBuffer.info(ExBuffer, partition: 0)
      iex> length
      1
  """
  @spec info(GenServer.server(), keyword()) :: [map()]
  def info(buffer, opts \\ []) do
    with {:ok, {_, parts}} <- fetch_buffer(buffer),
         {:ok, part} <- validate_partition(opts, parts) do
      Enum.map(enumerate_parts(parts, part), fn part ->
        do_part(buffer, part, &Partition.info/1)
      end)
    else
      {:error, reason} -> raise(ArgumentError, to_message(reason))
    end
  end

  @doc """
  Inserts the given item into the given `ExBuffer` based on the partitioner that the
  given `ExBuffer` was started with.

  ## Example

      iex> ExBuffer.insert(ExBuffer, "foo")
      :ok
  """
  @spec insert(GenServer.server(), term()) :: :ok
  def insert(buffer, item) do
    case fetch_buffer(buffer) do
      {:ok, {partitioner, _}} -> do_part(buffer, partitioner.(), &Partition.insert(&1, item))
      {:error, reason} -> raise(ArgumentError, to_message(reason))
    end
  end

  @doc """
  Inserts the given batch of items into the given `ExBuffer` based on the partitioner that
  the given `ExBuffer` was started with. This function returns the number of items that were
  inserted.

  All items in the batch are inserted into the same partition.

  > #### Tip {: .tip}
  >
  > When inserting multiple items into an `ExBuffer`, this function will be far more performant
  > than calling `ExBuffer.insert/2` for each one. As such, whenever items become available in
  > batches, this function should be preferred.

  ## Options

  A batch of items can be inserted into an `ExBuffer` with the following options:

    * `:flush_mode` - A value denoting whether how buffer will be flushed (if applicable).
      Possible values are `:sync` and `:async`. By default, this value is `:sync`, meaning
      that, if a flush condition is met while inserting items, the `ExBuffer` partition will
      synchronously flush before continuing to insert items. If this value is set to `:async`,
      all items will be inserted before checking if any flush conditions have been met.
      Afterwards, if a flush condition has been met, the `ExBuffer` partition will be flushed
      asynchronously. (Optional)

  ## Example

      iex> ExBuffer.insert_batch(ExBuffer, ["foo", "bar", "baz"])
      3
  """
  @spec insert_batch(GenServer.server(), Enumerable.t(), keyword()) :: non_neg_integer()
  def insert_batch(buffer, items, opts \\ []) do
    case fetch_buffer(buffer) do
      {:ok, {partitioner, _}} ->
        do_part(buffer, partitioner.(), &Partition.insert_batch(&1, items, opts))

      {:error, reason} ->
        raise(ArgumentError, to_message(reason))
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

  defp validate_partitioner(opts) do
    case Keyword.get(opts, :partitioner, :rotating) do
      partitioner when partitioner in [:random, :rotating] -> {:ok, partitioner}
      _ -> {:error, :invalid_partitioner}
    end
  end

  defp validate_partitions(opts) do
    case Keyword.get(opts, :partitions, 1) do
      parts when is_integer(parts) and parts > 0 -> {:ok, parts}
      _ -> {:error, :invalid_partitions}
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
    child_spec = {Partition, buffer_opts}

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
      nil -> {:error, :buffer_not_found}
      buffer -> {:ok, buffer}
    end
  end

  defp build_key(buffer), do: {__MODULE__, buffer}

  defp enumerate_parts(parts, :all), do: 0..(parts - 1)
  defp enumerate_parts(_, part), do: [part]

  defp do_part(buffer, partition, fun) do
    buffer
    |> partition_name(partition)
    |> fun.()
  end

  defp partition_name(buffer, partition) do
    {:via, PartitionSupervisor, {buffer, partition}}
  end

  defp to_message(reason), do: String.replace(to_string(reason), "_", " ")
end
