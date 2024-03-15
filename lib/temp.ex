defmodule Temp do
  @moduledoc """
  TODO(Gordon) - Add this
  TODO(Gordon) - Validate opts in outer module
  TODO(Gordon) - avoid time-based flush if table is empty (ignore_flush opt?)
  TODO(Gordon) - Add support for flush timeout?
  TODO(Gordon) - support track_size in outer module
  TODO(Gordon) - validate opts in function calls in outer module?
  TODO(Gordon) - Explicitly handle errors and exits in flush callback?
  TODO(Gordon) - better handling of partitioner with a single partition
  TODO(Gordon) - Rethink usage of nimble_options?
  TODO(Gordon) - validate opts
  TODO(Gordon) - think more carefully about round vs. ceil for post jitter
  """

  # @opts [
  #   :flush_callback,
  #   :flush_meta,
  #   :jitter_rate,
  #   :max_length,
  #   :max_size,
  #   :name,
  #   :partitioner,
  #   :partitions,
  #   :size_callback,
  #   :timeout,
  #   :track_size
  # ]

  ################################
  # Types
  ################################

  ################################
  # Callbacks
  ################################

  ################################
  # Public API
  ################################

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc """
  TODO(Gordon) - Add this
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  @spec start_link(module(), keyword()) :: Supervisor.on_start()
  def start_link(module \\ nil, opts) do
    # Validate opts
    # Build and persist partitioner
    # Start partition supervisor
    _opts = update_opts(module, opts)
    :ignore
  end

  # TODO(Gordon) - dump

  # TODO(Gordon) - flush

  # TODO(Gordon) - info

  # TODO(Gordon) - insert

  # TODO(Gordon) - insert_batch

  # TODO(Gordon) - __using__

  ################################
  # Private API
  ################################

  defp update_opts(nil, opts), do: opts

  defp update_opts(module, opts) do
    callbacks = [
      flush_callback: &module.handle_flush/2,
      size_callback: &module.handle_size/1
    ]

    opts
    |> Keyword.merge(callbacks)
    |> Keyword.put_new(:name, module)
  end

  # defp build_supervisor_opts(opts) do
  #   # TODO(Gordon) - implement this
  #   # Supervisor opts: jitter_rate, name, partitioner, partitions
  #   # 1. Remove supervisor opts and limits from opts
  #   # 2. Define with_args that adds the partition key and jitters limits
  #   # 3. Define child_spec
  #   # 4. Return newly created opts
  #   opts
  # end

  # TODO(Gordon) - switch jitter and limit in function signature
  # defp jitter_limit(_, :infinity), do: :infinity
  # defp jitter_limit(jitter, limit) when jitter == 0, do: limit

  # defp jitter_limit(jitter, limit) do
  #   ceil(limit * (1 - jitter * :rand.uniform()))
  # end

  # defp build_partitioner(_, 1), do: 0

  # defp build_partitioner(:random, parts) do
  #   fn -> :rand.uniform(parts) - 1 end
  # end

  # defp build_partitioner(:rotating, parts) do
  #   ref = :atomics.new(1, [])
  #   :atomics.put(ref, 1, -1)

  #   fn ->
  #     with p when p >= parts <- :atomics.add_get(ref, 1, 1) do
  #       :atomics.put(ref, 1, 0)
  #       0
  #     end
  #   end
  # end

  # defp next_partition(part) when is_integer(part), do: part
  # defp next_partition(partitioner), do: partitioner.()
end
