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

  @type callback :: function() | nil
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
  @spec insert(t(), term()) :: {:flush, t()} | {:cont, t()}
  def insert(state, item) do
    state = %{
      state
      | buffer: [item | state.buffer],
        length: state.length + 1,
        size: state.size + item_size(item)
    }

    if flush?(state), do: {:flush, state}, else: {:cont, state}
  end

  @doc false
  @spec items(t()) :: list()
  def items(state), do: Enum.reverse(state.buffer)

  @doc false
  @spec new(callback(), limit(), limit(), limit()) ::
          {:ok, t()} | {:error, :invalid_callback | :invalid_limit}
  def new(callback, max_length, max_size, timeout) do
    with :ok <- validate_callback(callback),
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

  ################################
  # Private API
  ################################

  defp validate_callback(nil), do: :ok
  defp validate_callback(fun) when is_function(fun, @callback_arity), do: :ok
  defp validate_callback(_), do: {:error, :invalid_callback}

  defp validate_limit(:infinity), do: :ok
  defp validate_limit(limit) when is_integer(limit) and limit >= 0, do: :ok
  defp validate_limit(_), do: {:error, :invalid_limit}

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
