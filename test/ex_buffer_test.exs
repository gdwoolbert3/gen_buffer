defmodule ExBufferTest do
  use ExUnit.Case, async: true

  setup do
    buffer = :test_buffer
    destination = self()
    callback = fn data -> send(destination, {:data, data}) end
    %{buffer: buffer, opts: [name: buffer, callback: callback]}
  end

  describe "start_link/1" do
    test "will start an ExBuffer", ctx do
      start_supervised!({ExBuffer, ctx.opts})
    end

    test "will not start an ExBuffer with an invalid callback", ctx do
      opts = Keyword.put(ctx.opts, :callback, fn x, y, z -> x + y + z end)

      assert {:error, {:invalid_callback, _}} = start_supervised({ExBuffer, opts})
      refute_receive {:data, _}
    end

    test "will not start an ExBuffer with an invalid limit", ctx do
      opts = Keyword.put(ctx.opts, :max_length, -5)

      assert {:error, {:invalid_limit, _}} = start_supervised({ExBuffer, opts})
      refute_receive {:data, _}
    end

    test "will flush an ExBuffer on termination", ctx do
      start_supervised!({ExBuffer, ctx.opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert GenServer.stop(ctx.buffer) == :ok
      assert_receive {:data, ["foo", "bar", "baz"]}
    end
  end

  describe "chunk_enum!/2" do
    test "will correctly chunk an enumerable" do
      enum = ["foo", "bar", "baz", "foobar", "barbaz", "foobarbaz"]
      enum = ExBuffer.chunk_enum!(enum, max_length: 3, max_size: 10)

      assert Enum.into(enum, []) == [["foo", "bar", "baz"], ["foobar", "barbaz"], ["foobarbaz"]]
    end

    test "will raise an error with an invalid limit" do
      chunk_enum = fn -> ExBuffer.chunk_enum!(["foo", "bar", "baz"], max_length: -5) end

      assert_raise ArgumentError, "invalid limit", chunk_enum
    end
  end

  describe "dump/1" do
    test "will dump the contents of an ExBuffer", ctx do
      start_supervised!({ExBuffer, ctx.opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert ExBuffer.dump(ctx.buffer) == ["foo", "bar", "baz"]
      assert ExBuffer.length(ctx.buffer) == 0
    end
  end

  describe "flush/1" do
    test "will flush an ExBuffer regardless of conditions being met", ctx do
      opts = Keyword.merge(ctx.opts, max_length: 5, max_size: 20, buffer_timeout: 1_000)
      start_supervised!({ExBuffer, opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert ExBuffer.flush(ctx.buffer) == :ok
      assert_receive {:data, ["foo", "bar", "baz"]}
    end

    test "will synchronously flush an ExBuffer", ctx do
      start_supervised!({ExBuffer, ctx.opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert ExBuffer.flush(ctx.buffer, async: false) == :ok
      assert_receive {:data, ["foo", "bar", "baz"]}
    end
  end

  describe "insert/2" do
    test "will correctly insert data into an ExBuffer", ctx do
      start_supervised!({ExBuffer, ctx.opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert ExBuffer.dump(ctx.buffer) == ["foo", "bar", "baz"]
    end

    test "will flush an ExBuffer after hitting max length", ctx do
      opts = Keyword.put(ctx.opts, :max_length, 3)
      start_supervised!({ExBuffer, opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert_receive {:data, ["foo", "bar", "baz"]}
    end

    test "will flush an ExBuffer after hitting max size", ctx do
      opts = Keyword.put(ctx.opts, :max_size, 9)
      start_supervised!({ExBuffer, opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert_receive {:data, ["foo", "bar", "baz"]}
    end

    test "will flush an ExBuffer after exceeding timeout", ctx do
      opts = Keyword.put(ctx.opts, :buffer_timeout, 100)
      start_supervised!({ExBuffer, opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert_receive {:data, ["foo", "bar", "baz"]}, 150
    end

    test "will flush an ExBuffer when first condition is met", ctx do
      opts = Keyword.merge(ctx.opts, max_length: 3, max_size: 10)
      start_supervised!({ExBuffer, opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert_receive {:data, ["foo", "bar", "baz"]}
    end
  end

  describe "length/1" do
    test "will return the length of an ExBuffer", ctx do
      start_supervised!({ExBuffer, ctx.opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert ExBuffer.length(ctx.buffer) == 3
    end
  end

  describe "size/1" do
    test "will return the size of an ExBuffer", ctx do
      opts = Keyword.put(ctx.opts, :max_size, 10)
      start_supervised!({ExBuffer, opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert ExBuffer.size(ctx.buffer) == 9
    end

    test "will return the size of an ExBuffer without a max size", ctx do
      start_supervised!({ExBuffer, ctx.opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert ExBuffer.size(ctx.buffer) == 9
    end
  end
end
