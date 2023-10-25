defmodule ExBuffer do
  @moduledoc """
  An ExBuffer is a process that maintains a collection of items and flushes
  them once certain conditions have been met.

  ExBuffers can flush based on a timeout, a maximum length (item count), a
  maximum byte size, or a combination of the three. When multiple conditions are
  used, the ExBuffer will flush when the **first** condition is met.

  ExBuffers also come with a number of helpful tools for testing and debugging.
  """

  alias ExBuffer.Buffer.{Server, Stream}

  @type error :: :invalid_callback | :invalid_limit

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
  defdelegate start_link(opts \\ []), to: Server

  @doc """
  Lazily chunks an enumerable based on ExBuffer flush conditions.

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
  Lazily chunks an enumerable based on ExBuffer flush conditions and raises an `ArgumentError`
  with invalid options.

  For more information on this function's usage, purpose, and options, see `ExBuffer.chunk!/2`.

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
  @spec dump(GenServer.server()) :: list()
  defdelegate dump(buffer), to: Server

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
  defdelegate flush(buffer, opts \\ []), to: Server

  @doc """
  Inserts the given item into the given `ExBuffer`.

  ## Example

      iex> ExBuffer.insert(:buffer, "foo")
      :ok
  """
  @spec insert(GenServer.server(), term()) :: :ok
  defdelegate insert(buffer, item), to: Server

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
  defdelegate length(buffer), to: Server

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
  defdelegate next_flush(buffer), to: Server

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
  defdelegate size(buffer), to: Server

  @doc false
  @spec child_spec(keyword()) :: map()
  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end
end
