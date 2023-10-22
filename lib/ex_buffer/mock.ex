defmodule ExBuffer.Mock do
  @moduledoc false

  alias ExBuffer.State

  ################################
  # Public API
  ################################

  @doc false
  @spec chunk!(Enumerable.t(), keyword()) :: Enumerable.t()
  def chunk!(enum, opts \\ []) do
    case init_state(opts) do
      {:ok, state} -> Stream.chunk_while(enum, state, &do_insert(&2, &1), &buffer_end/1)
      {:error, reason} -> raise(ArgumentError, to_message(reason))
    end
  end

  ################################
  # Private API
  ################################

  defp init_state(opts) do
    max_length = Keyword.get(opts, :max_length, :infinity)
    max_size = Keyword.get(opts, :max_size, :infinity)
    State.new(nil, max_length, max_size, :infinity)
  end

  defp do_insert(state, item) do
    with {:flush, state} <- State.insert(state, item) do
      {:cont, State.items(state), State.refresh(state)}
    end
  end

  defp buffer_end(%State{buffer: []} = state), do: {:cont, state}
  defp buffer_end(state), do: {:cont, State.items(state), State.refresh(state)}

  defp to_message(reason), do: String.replace(to_string(reason), "_", " ")
end
