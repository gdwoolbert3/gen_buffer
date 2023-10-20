# GenBuffer

A GenBuffer is a process that maintains a collection of items and flushes them once certain conditions have been met.

## Installation

TODO(Gordon) - Add this

## Documentation

TODO(Gordon) - Add this

## Getting Started

We can easily start a GenBuffer by adding it directly to a supervision tree.

```elixir
opts = [callback: &IO.inspect/1, buffer_timeout: 5_000, name: :test_buffer]

children = [
  {GenBuffer, opts}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

Once the buffer has been started, we can insert items.

```elixir
GenBuffer.insert(:test_buffer, "foo")
GenBuffer.insert(:test_buffer, "bar")
```

And, once any of the configured conditions have been met, the buffer will automatically flush.

```elixir
# 5 seconds after previous flush
#=> outputs ["foo", "bar"]

GenBuffer.insert(:test_buffer, "baz")

# 5 seconds after previous flush
#=> outputs ["baz"]
```
