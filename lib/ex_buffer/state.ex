defmodule ExBuffer.State do
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

  @type limit :: non_neg_integer() | :infinity

  @type t :: %__MODULE__{
          callback: function(),
          max_length: limit(),
          max_size: limit(),
          timeout: limit(),
          buffer: list(),
          length: non_neg_integer(),
          size: non_neg_integer(),
          timer: reference() | nil
        }

  @callback_arity 1

  ################################
  # Public API
  ################################

  @doc false
  @spec flush?(t()) :: boolean()
  def flush?(state) do
    exceeds?(state.length, state.max_length) or exceeds?(state.size, state.max_size)
  end

  @doc false
  @spec insert(t(), term()) :: t()
  def insert(state, item) do
    buffer = [item | state.buffer]
    length = state.length + 1
    size = state.size + maybe_item_size(state, item)
    %{state | buffer: buffer, length: length, size: size}
  end

  @doc false
  @spec items(t()) :: list()
  def items(state), do: Enum.reverse(state.buffer)

  @doc false
  @spec length(t()) :: non_neg_integer()
  def length(state), do: state.length

  @doc false
  @spec new(function(), limit(), limit(), limit(), boolean()) ::
          {:ok, t()} | {:error, :invalid_callback | :invalid_limit}
  def new(callback, max_length, max_size, timeout, process) do
    with :ok <- validate_callback(callback, process),
         :ok <- validate_limit(max_length),
         :ok <- validate_limit(max_size),
         :ok <- validate_limit(timeout) do
      state = %__MODULE__{
        callback: callback,
        max_length: max_length,
        max_size: max_size,
        timeout: timeout
      }

      {:ok, state}
    end
  end

  @doc false
  @spec refresh(t(), reference() | nil) :: t()
  def refresh(state, timer \\ nil) do
    %{state | buffer: [], length: 0, size: 0, timer: timer}
  end

  @doc false
  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{buffer: buffer, max_size: :infinity}) do
    Enum.reduce(buffer, 0, fn item, acc -> item_size(item) + acc end)
  end

  def size(state), do: state.size

  ################################
  # Private API
  ################################

  defp validate_callback(_, false), do: :ok
  defp validate_callback(fun, _) when is_function(fun, @callback_arity), do: :ok
  defp validate_callback(_, _), do: {:error, :invalid_callback}

  defp validate_limit(:infinity), do: :ok
  defp validate_limit(limit) when is_integer(limit) and limit >= 0, do: :ok
  defp validate_limit(_), do: {:error, :invalid_limit}

  defp maybe_item_size(%__MODULE__{max_size: :infinity}, _), do: 0
  defp maybe_item_size(_, item), do: item_size(item)

  defp item_size(item) when is_bitstring(item), do: byte_size(item)

  defp item_size(item) do
    item
    |> :erlang.term_to_binary()
    |> byte_size()
  end

  defp exceeds?(_, :infinity), do: false
  defp exceeds?(num, max), do: num >= max
end
