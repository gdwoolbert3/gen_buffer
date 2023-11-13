defmodule ExBuffer.TestBuffer do
  @moduledoc false

  use ExBuffer

  ################################
  # Public API
  ################################

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    ExBuffer.start_link(__MODULE__, opts)
  end

  ################################
  # ExBuffer Callbacks
  ################################

  @doc false
  @impl ExBuffer
  @spec handle_flush(list(), keyword()) :: :ok
  def handle_flush(data, opts) do
    pid = Keyword.get(opts, :meta)
    send(pid, {:impl_mod, data, opts})
    :ok
  end

  @doc false
  @impl ExBuffer
  @spec handle_size(term()) :: non_neg_integer()
  def handle_size(item) do
    byte_size(item) + 1
  end
end
