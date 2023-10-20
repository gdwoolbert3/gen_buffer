defmodule GenBufferTest do
  use ExUnit.Case, async: true

  setup do
    buffer = :test_buffer
    destination = self()
    callback = fn data -> send(destination, {:data, data}) end
    %{buffer: buffer, opts: [name: buffer, callback: callback]}
  end

  describe "start_link/1" do
    test "will start a GenBuffer", ctx do
      start_supervised!({GenBuffer, ctx.opts})
    end

    test "will not start a GenBuffer with an invalid callback", ctx do
      opts = Keyword.put(ctx.opts, :callback, fn x, y, z -> x + y + z end)

      assert {:error, {:invalid_callback, _}} = start_supervised({GenBuffer, opts})
      refute_receive {:data, _}
    end

    test "will not start a GenBuffer with an invalid limit", ctx do
      opts = Keyword.put(ctx.opts, :max_length, -5)

      assert {:error, {:invalid_limit, _}} = start_supervised({GenBuffer, opts})
      refute_receive {:data, _}
    end

    test "will flush a GenBuffer on termination", ctx do
      start_supervised!({GenBuffer, ctx.opts})

      assert GenBuffer.insert(ctx.buffer, "foo") == :ok
      assert GenBuffer.insert(ctx.buffer, "bar") == :ok
      assert GenBuffer.insert(ctx.buffer, "baz") == :ok
      assert GenServer.stop(ctx.buffer) == :ok
      assert_receive {:data, ["foo", "bar", "baz"]}
    end
  end

  describe "chunk_enum!/2" do
    test "will correctly chunk an enumerable" do
      enum = ["foo", "bar", "baz", "foobar", "barbaz", "foobarbaz"]
      enum = GenBuffer.chunk_enum!(enum, max_length: 3, max_size: 10)

      assert Enum.into(enum, []) == [["foo", "bar", "baz"], ["foobar", "barbaz"], ["foobarbaz"]]
    end

    test "will raise an error with an invalid limit" do
      chunk_enum = fn -> GenBuffer.chunk_enum!(["foo", "bar", "baz"], max_length: -5) end

      assert_raise ArgumentError, "invalid limit", chunk_enum
    end
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

    test "will synchronously flush a GenBuffer", ctx do
      start_supervised!({GenBuffer, ctx.opts})

      assert GenBuffer.insert(ctx.buffer, "foo") == :ok
      assert GenBuffer.insert(ctx.buffer, "bar") == :ok
      assert GenBuffer.insert(ctx.buffer, "baz") == :ok
      assert GenBuffer.flush(ctx.buffer, async: false) == :ok
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
      opts = Keyword.put(ctx.opts, :max_size, 10)
      start_supervised!({GenBuffer, opts})

      assert GenBuffer.insert(ctx.buffer, "foo") == :ok
      assert GenBuffer.insert(ctx.buffer, "bar") == :ok
      assert GenBuffer.insert(ctx.buffer, "baz") == :ok
      assert GenBuffer.size(ctx.buffer) == 9
    end

    test "will return the size of a GenBuffer without a max size", ctx do
      start_supervised!({GenBuffer, ctx.opts})

      assert GenBuffer.insert(ctx.buffer, "foo") == :ok
      assert GenBuffer.insert(ctx.buffer, "bar") == :ok
      assert GenBuffer.insert(ctx.buffer, "baz") == :ok
      assert GenBuffer.size(ctx.buffer) == 9
    end
  end
end
