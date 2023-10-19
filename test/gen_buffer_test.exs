defmodule GenBufferTest do
  use ExUnit.Case

  setup do
    buffer = :test_buffer
    destination = self()
    callback = fn data -> send(destination, {:data, data}) end
    %{buffer: buffer, opts: [name: buffer, callback: callback]}
  end

  describe "dump/1" do
    test "will dump the contents of a GenBuffer", ctx do
      start_supervised!({GenBuffer, ctx.opts})

      assert GenBuffer.insert(ctx.buffer, "foo") == :ok
      assert GenBuffer.insert(ctx.buffer, "bar") == :ok
      assert GenBuffer.insert(ctx.buffer, "baz") == :ok
      assert GenBuffer.dump(ctx.buffer) == ["foo", "bar", "baz"]
      assert GenBuffer.length(ctx.buffer) == 0
    end
  end

  describe "flush/1" do
    test "will flush a GenBuffer regardless of conditions being met", ctx do
      opts = Keyword.merge(ctx.opts, max_length: 5, max_size: 20, buffer_timeout: 1_000)
      start_supervised!({GenBuffer, opts})

      assert GenBuffer.insert(ctx.buffer, "foo") == :ok
      assert GenBuffer.insert(ctx.buffer, "bar") == :ok
      assert GenBuffer.insert(ctx.buffer, "baz") == :ok
      assert GenBuffer.flush(ctx.buffer) == :ok
      assert_receive {:data, ["foo", "bar", "baz"]}
    end
  end

  describe "insert/2" do
    test "will correctly insert data into a GenBuffer", ctx do
      start_supervised!({GenBuffer, ctx.opts})

      assert GenBuffer.insert(ctx.buffer, "foo") == :ok
      assert GenBuffer.insert(ctx.buffer, "bar") == :ok
      assert GenBuffer.insert(ctx.buffer, "baz") == :ok
      assert GenBuffer.dump(ctx.buffer) == ["foo", "bar", "baz"]
    end

    test "will flush a GenBuffer after hitting max length", ctx do
      opts = Keyword.put(ctx.opts, :max_length, 3)
      start_supervised!({GenBuffer, opts})

      assert GenBuffer.insert(ctx.buffer, "foo") == :ok
      assert GenBuffer.insert(ctx.buffer, "bar") == :ok
      assert GenBuffer.insert(ctx.buffer, "baz") == :ok
      assert_receive {:data, ["foo", "bar", "baz"]}
    end

    test "will flush a GenBuffer after hitting max size", ctx do
      opts = Keyword.put(ctx.opts, :max_size, 9)
      start_supervised!({GenBuffer, opts})

      assert GenBuffer.insert(ctx.buffer, "foo") == :ok
      assert GenBuffer.insert(ctx.buffer, "bar") == :ok
      assert GenBuffer.insert(ctx.buffer, "baz") == :ok
      assert_receive {:data, ["foo", "bar", "baz"]}
    end

    test "will flush a GenBuffer after exceeding timeout", ctx do
      opts = Keyword.put(ctx.opts, :buffer_timeout, 100)
      start_supervised!({GenBuffer, opts})

      assert GenBuffer.insert(ctx.buffer, "foo") == :ok
      assert GenBuffer.insert(ctx.buffer, "bar") == :ok
      assert GenBuffer.insert(ctx.buffer, "baz") == :ok
      assert_receive {:data, ["foo", "bar", "baz"]}, 150
    end

    test "will flush a GenBuffer when first condition is met", ctx do
      opts = Keyword.merge(ctx.opts, max_length: 3, max_size: 10)
      start_supervised!({GenBuffer, opts})

      assert GenBuffer.insert(ctx.buffer, "foo") == :ok
      assert GenBuffer.insert(ctx.buffer, "bar") == :ok
      assert GenBuffer.insert(ctx.buffer, "baz") == :ok
      assert_receive {:data, ["foo", "bar", "baz"]}
    end
  end

  describe "length/1" do
    test "will return the length of a GenBuffer", ctx do
      start_supervised!({GenBuffer, ctx.opts})

      assert GenBuffer.insert(ctx.buffer, "foo") == :ok
      assert GenBuffer.insert(ctx.buffer, "bar") == :ok
      assert GenBuffer.insert(ctx.buffer, "baz") == :ok
      assert GenBuffer.length(ctx.buffer) == 3
    end
  end

  describe "size/1" do
    test "will return the size of a GenBuffer", ctx do
      start_supervised!({GenBuffer, ctx.opts})

      assert GenBuffer.insert(ctx.buffer, "foo") == :ok
      assert GenBuffer.insert(ctx.buffer, "bar") == :ok
      assert GenBuffer.insert(ctx.buffer, "baz") == :ok
      assert GenBuffer.size(ctx.buffer) == 9
    end
  end
end
