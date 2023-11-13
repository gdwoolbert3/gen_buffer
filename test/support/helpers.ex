defmodule ExBuffer.Helpers do
  @moduledoc false

  import ExUnit.Callbacks, only: [start_supervised: 1]

  ################################
  # Public API
  ################################

  @doc false
  @spec seed_buffer(GenServer.server()) :: :ok
  def seed_buffer(buffer) do
    ExBuffer.insert(buffer, "foo")
    ExBuffer.insert(buffer, "bar")
    ExBuffer.insert(buffer, "baz")
  end

  @doc false
  @spec start_ex_buffer(keyword()) :: {:ok, GenServer.name()} | {:error, ExBuffer.error()}
  def start_ex_buffer(opts \\ []) do
    name = Keyword.get(opts, :name, ExBuffer)
    opts = Keyword.put_new(opts, :flush_callback, flush_callback(name))

    case start_supervised({ExBuffer, opts}) do
      {:ok, pid} -> {:ok, process_name(pid)}
      {:error, {reason, _}} -> {:error, reason}
    end
  end

  @doc false
  @spec start_test_buffer(keyword()) :: {:ok, GenServer.name()} | {:error, ExBuffer.error()}
  def start_test_buffer(opts \\ []) do
    opts = Keyword.put(opts, :flush_meta, self())

    case start_supervised({ExBuffer.TestBuffer, opts}) do
      {:ok, pid} -> {:ok, process_name(pid)}
      {:error, {reason, _}} -> {:error, reason}
    end
  end

  ################################
  # Private API
  ################################

  defp flush_callback(name) do
    destination = self()
    fn data, opts -> send(destination, {name, data, opts}) end
  end

  defp process_name(pid) do
    # TODO(Gordon) - handle case where info is nil?
    pid
    |> Process.info()
    |> Keyword.get(:registered_name)
  end
end
