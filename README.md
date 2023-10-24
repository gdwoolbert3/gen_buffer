# ExBuffer

![CI](https://github.com/gdwoolbert3/ex_buffer/actions/workflows/ci.yml/badge.svg)
[![Package](https://img.shields.io/hexpm/v/ex_buffer.svg)](https://hex.pm/packages/ex_buffer)

An ExBuffer is a process that maintains a collection of items and flushes them once certain conditions have been met.

## Installation

This package can be installed by adding `:ex_buffer` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ex_buffer "~> 0.2.1"}
  ]
end
```

## Documentation

For additional documentation, see [HexDocs](https://hexdocs.pm/ex_buffer/readme.html).

## Getting Started

We can easily start an ExBuffer by adding it directly to a supervision tree.

```elixir
opts = [callback: &IO.inspect/1, max_length: 3, name: :buffer]

children = [
  {ExBuffer, opts}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

Once the buffer has been started, we can insert items.

```elixir
ExBuffer.insert(:buffer, "foo")
ExBuffer.insert(:buffer, "bar")
```

And, once any of the configured conditions have been met, the buffer will automatically flush.

```elixir
ExBuffer.insert(:buffer, "baz")
# ExBuffer flushes asynchronously and outputs ["foo", "bar", "baz"]
```

## Example

`ExBuffer` is designed to be customizable, allowing it to be used in any number of scenarios. For example, we can
use it in conjunction with Elixir's `PartitionSupervisor` to easily create a partitioned buffer with dynamic flush
behavior.

```elixir
defmodule PartitionedBuffer do
  use Supervisor

  def start_link(opts \\ []) do
    ex_buffer_opts = [
      flush_callback: &handle_flush/2,
      max_length: 3
    ]

    opts = Keyword.merge(ex_buffer_opts, opts)
    Supervisor.start_link(__MODULE__, ex_buffer_opts, name: __MODULE__)
  end

  def insert(item, partition) do
    ExBuffer.insert({:via, PartitionSupervisor, {:buffer, partition}}, item)
  end

  @impl Supervisor
  def init(opts) do
    part_sup_opts = [
      name: :buffer,
      child_spec: {ExBuffer, opts},
      partitions: 2,
      with_arguments: fn [opts], part -> [Keyword.put(opts, :flush_meta, part)] end
    ]

    children = [
      {PartitionSupervisor, part_sup_opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp handle_flush(data, opts) do
    partition = Keyword.get(opts, :meta)
    IO.inspect({partition, data, size})
  end
end
```

We can easily start the `PartitionedBuffer` process from above to see it in action.

```elixir
PartitionedBuffer.start_link()

PartitionedBuffer.insert("foo", 0)
PartitionedBuffer.insert("foo", 1)
PartitionedBuffer.insert("bar", 0)
PartitionedBuffer.insert("bar", 1)
PartitionedBuffer.insert("baz", 0)
# ExBuffer flushes asynchronously and outputs {0, ["foo", "bar", "baz"]}

PartitionedBuffer.insert("baz", 1)
# ExBuffer flushes asynchronously and outputs {1, ["foo", "bar", "baz"]}
```
