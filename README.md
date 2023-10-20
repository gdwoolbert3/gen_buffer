# ExBuffer

![CI](https://github.com/gdwoolbert3/ex_buffer/actions/workflows/ci.yml/badge.svg)
[![Package](https://img.shields.io/hexpm/v/ex_buffer.svg)](https://hex.pm/packages/ex_buffer)

An ExBuffer is a process that maintains a collection of items and flushes them once certain conditions have been met.

## Installation

This package can be installed by adding `:ex_buffer` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ex_buffer "~> 0.1.0"}
  ]
end
```

## Documentation

For additional documentation, see [HexDocs](https://hexdocs.pm/ex_buffer/readme.html).

## Getting Started

We can easily start an ExBuffer by adding it directly to a supervision tree.

```elixir
opts = [callback: &IO.inspect/1, buffer_timeout: 5_000, name: :test_buffer]

children = [
  {ExBuffer, opts}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

Once the buffer has been started, we can insert items.

```elixir
ExBuffer.insert(:test_buffer, "foo")
ExBuffer.insert(:test_buffer, "bar")
```

And, once any of the configured conditions have been met, the buffer will automatically flush.

```elixir
# 5 seconds after previous flush
#=> outputs ["foo", "bar"]

ExBuffer.insert(:test_buffer, "baz")

# 5 seconds after previous flush
#=> outputs ["baz"]
```
