defmodule ExBufferTest do
  use ExUnit.Case, async: true
  doctest ExBuffer

  setup %{test_type: test_type} do
    # This `setup` callback is responsible for both tests and doctests.
    # Normal tests expect a map with a buffer name and default opts.
    # Doctests only expect a running ExBuffer named `:buffer`.
    case test_type do
      :test ->
        destination = self()
        flush_callback = fn data, opts -> send(destination, {:data, data, opts}) end
        %{buffer: :buffer, opts: [name: :buffer, flush_callback: flush_callback]}

      :doctest ->
        opts = [flush_callback: fn _, _ -> :ok end, name: :buffer, buffer_timeout: 5_000]
        start_supervised!({ExBuffer, opts})
        :ok
    end
  end

  describe "start_link/1" do
    test "will start an ExBuffer", ctx do
      start_supervised!({ExBuffer, ctx.opts})
    end

    test "will not start an ExBuffer without a callback", ctx do
      opts = Keyword.delete(ctx.opts, :flush_callback)

      assert {:error, {:invalid_callback, _}} = start_supervised({ExBuffer, opts})
      refute_receive {:data, _, _}
    end

    test "will not start an ExBuffer with an invalid flush callback", ctx do
      opts = Keyword.put(ctx.opts, :flush_callback, fn x, y, z -> x + y + z end)

      assert {:error, {:invalid_callback, _}} = start_supervised({ExBuffer, opts})
      refute_receive {:data, _, _}
    end

    test "will not start an ExBuffer with an invalid limit", ctx do
      opts = Keyword.put(ctx.opts, :max_length, -5)

      assert {:error, {:invalid_limit, _}} = start_supervised({ExBuffer, opts})
      refute_receive {:data, _, _}
    end

    test "will not start an ExBuffer with an invalid size callback", ctx do
      opts = Keyword.put(ctx.opts, :size_callback, nil)

      assert {:error, {:invalid_callback, _}} = start_supervised({ExBuffer, opts})
      refute_receive {:data, _, _}
    end

    test "will flush an ExBuffer on termination", ctx do
      start_supervised!({ExBuffer, ctx.opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert GenServer.stop(ctx.buffer) == :ok
      assert_receive {:data, ["foo", "bar", "baz"], _}
    end
  end

  describe "chunk/2" do
    test "will correctly chunk an enumerable" do
      enum = ["foo", "bar", "baz", "foobar", "barbaz", "foobarbaz"]

      assert {:ok, enum} = ExBuffer.chunk(enum, max_length: 3, max_size: 10)
      assert Enum.into(enum, []) == [["foo", "bar", "baz"], ["foobar", "barbaz"], ["foobarbaz"]]
    end

    test "will correctly chunk an enumerable with a size callback" do
      enum = ["foo", "bar", "baz"]

      assert {:ok, enum} = ExBuffer.chunk(enum, max_size: 8, size_callback: &(byte_size(&1) + 1))
      assert Enum.into(enum, []) == [["foo", "bar"], ["baz"]]
    end

    test "will return an error with an invalid callback" do
      callback = fn -> :ok end
      enum = ["foo", "bar", "baz"]

      assert ExBuffer.chunk(enum, size_callback: callback) == {:error, :invalid_callback}
    end

    test "will return an error with an invalid limit" do
      assert ExBuffer.chunk(["foo", "bar", "baz"], max_length: -5) == {:error, :invalid_limit}
    end
  end

  describe "chunk!/2" do
    test "will correctly chunk an enumerable" do
      enum = ["foo", "bar", "baz", "foobar", "barbaz", "foobarbaz"]
      enum = ExBuffer.chunk!(enum, max_length: 3, max_size: 10)

      assert Enum.into(enum, []) == [["foo", "bar", "baz"], ["foobar", "barbaz"], ["foobarbaz"]]
    end

    test "will correctly chunk an enumerable with a size callback" do
      enum = ["foo", "bar", "baz"]
      enum = ExBuffer.chunk!(enum, max_size: 8, size_callback: &(byte_size(&1) + 1))

      assert Enum.into(enum, []) == [["foo", "bar"], ["baz"]]
    end

    test "will raise an error with an invalid callback" do
      fun = fn -> ExBuffer.chunk!(["foo", "bar", "baz"], size_callback: fn -> :ok end) end

      assert_raise ArgumentError, "invalid callback", fun
    end

    test "will raise an error with an invalid limit" do
      fun = fn -> ExBuffer.chunk!(["foo", "bar", "baz"], max_length: -5) end

      assert_raise ArgumentError, "invalid limit", fun
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
      assert_receive {:data, ["foo", "bar", "baz"], _}
    end

    test "will synchronously flush an ExBuffer", ctx do
      start_supervised!({ExBuffer, ctx.opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert ExBuffer.flush(ctx.buffer, async: false) == :ok
      assert_receive {:data, ["foo", "bar", "baz"], _}
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
      assert_receive {:data, ["foo", "bar", "baz"], _}
    end

    test "will flush an ExBuffer after hitting max size", ctx do
      opts = Keyword.put(ctx.opts, :max_size, 9)
      start_supervised!({ExBuffer, opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert_receive {:data, ["foo", "bar", "baz"], _}
    end

    test "will flush an ExBuffer with a size callback", ctx do
      opts = Keyword.merge(ctx.opts, max_size: 12, size_callback: &(byte_size(&1) + 1))
      start_supervised!({ExBuffer, opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert_receive {:data, ["foo", "bar", "baz"], _}
    end

    test "will flush an ExBuffer after exceeding timeout", ctx do
      opts = Keyword.put(ctx.opts, :buffer_timeout, 100)
      start_supervised!({ExBuffer, opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert_receive {:data, ["foo", "bar", "baz"], _}, 150
    end

    test "will flush an ExBuffer when first condition is met", ctx do
      opts = Keyword.merge(ctx.opts, max_length: 3, max_size: 10)
      start_supervised!({ExBuffer, opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert_receive {:data, ["foo", "bar", "baz"], _}
    end

    test "will include flush meta when an ExBuffer is flushed", ctx do
      opts = Keyword.merge(ctx.opts, max_length: 3, flush_meta: "meta")
      start_supervised!({ExBuffer, opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert_receive {:data, ["foo", "bar", "baz"], [length: 3, meta: "meta", size: 9]}
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

  describe "next_flush/1" do
    test "will return the time before the next flush", ctx do
      opts = Keyword.put(ctx.opts, :buffer_timeout, 1_000)
      start_supervised!({ExBuffer, opts})
      :timer.sleep(100)

      assert ExBuffer.next_flush(ctx.buffer) < 1_000
    end

    test "will return nil when buffer has no timeout", ctx do
      start_supervised!({ExBuffer, ctx.opts})

      assert is_nil(ExBuffer.next_flush(ctx.buffer))
    end
  end

  describe "size/1" do
    test "will return the size of an ExBuffer", ctx do
      start_supervised!({ExBuffer, ctx.opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert ExBuffer.size(ctx.buffer) == 9
    end

    test "will return the size of an ExBuffer with a size callback", ctx do
      opts = Keyword.merge(ctx.opts, size_callback: &(byte_size(&1) + 1))
      start_supervised!({ExBuffer, opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert ExBuffer.size(ctx.buffer) == 12
    end
  end
end
