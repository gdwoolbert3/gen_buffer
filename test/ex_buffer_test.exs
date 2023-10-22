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
        callback = fn data -> send(destination, {:data, data}) end
        %{buffer: :buffer, opts: [name: :buffer, callback: callback]}

      :doctest ->
        opts = [callback: fn _ -> :ok end, name: :buffer, buffer_timeout: 5_000]
        start_supervised!({ExBuffer, opts})
        :ok
    end
  end

  describe "start_link/1" do
    test "will start an ExBuffer", ctx do
      start_supervised!({ExBuffer, ctx.opts})
    end

    test "will not start an ExBuffer without a callback", ctx do
      opts = Keyword.delete(ctx.opts, :callback)

      assert {:error, {:invalid_callback, _}} = start_supervised({ExBuffer, opts})
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

  describe "chunk!/2" do
    test "will correctly chunk an enumerable" do
      enum = ["foo", "bar", "baz", "foobar", "barbaz", "foobarbaz"]
      enum = ExBuffer.chunk!(enum, max_length: 3, max_size: 10)

      assert Enum.into(enum, []) == [["foo", "bar", "baz"], ["foobar", "barbaz"], ["foobarbaz"]]
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
      opts = Keyword.put(ctx.opts, :max_size, 10)
      start_supervised!({ExBuffer, opts})

      assert ExBuffer.insert(ctx.buffer, "foo") == :ok
      assert ExBuffer.insert(ctx.buffer, "bar") == :ok
      assert ExBuffer.insert(ctx.buffer, "baz") == :ok
      assert ExBuffer.size(ctx.buffer) == 9
    end
  end
end
