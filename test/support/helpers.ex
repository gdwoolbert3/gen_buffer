defmodule ExBuffer.Helpers do
  @moduledoc false

  import ExUnit.Callbacks, only: [start_supervised: 1]

  @type error :: ExBuffer.Partition.error() | :invalid_partitions | :invalid_partitioner

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
  @spec start_ex_buffer(keyword()) :: {:ok, GenServer.name()} | {:error, error()}
  def start_ex_buffer(opts \\ []) do
    name = Keyword.get(opts, :name, ExBuffer)
    opts = Keyword.put_new(opts, :flush_callback, flush_callback(name))
    start_buffer({ExBuffer, opts})
  end

  @doc false
  @spec start_test_buffer(keyword()) :: {:ok, GenServer.name()} | {:error, error()}
  def start_test_buffer(opts \\ []) do
    opts = Keyword.put(opts, :flush_meta, self())
    start_buffer({ExBuffer.TestBuffer, opts})
  end

  ################################
  # Private API
  ################################

  defp flush_callback(name) do
    destination = self()
    fn data, opts -> send(destination, {name, data, opts}) end
  end

  defp start_buffer(child_spec) do
    case start_supervised(child_spec) do
      {:ok, pid} -> get_buffer_name(pid)
      {:error, {{_, {_, _, reason}}, _}} -> {:error, reason}
      {:error, {reason, _}} -> {:error, reason}
    end
  end

  defp get_buffer_name(pid) do
    pid
    |> Process.info()
    |> case do
      nil -> {:error, :not_found}
      info -> {:ok, Keyword.get(info, :registered_name)}
    end
  end
end
