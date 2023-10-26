defmodule ExBufferTest do
  use ExUnit.Case, async: true
  doctest ExBuffer

  import ExBuffer.Helpers

  setup %{test_type: test_type} do
    if test_type == :doctest do
      opts = [name: :buffer, flush_callback: fn _, _ -> :ok end, buffer_timeout: 5_000]
      if start_ex_buffer(opts) == {:ok, :buffer}, do: :ok
    else
      :ok
    end
  end

  describe "start_link/2" do
    test "will start an ExBuffer" do
      assert {:ok, _} = start_ex_buffer()
    end

    test "will start an ExBuffer from an implementation module" do
      assert {:ok, _} = start_test_buffer()
    end

    test "will not start an ExBuffer with an invalid flush callback" do
      opts = [flush_callback: fn x, y, z -> x + y + z end]

      assert start_ex_buffer(opts) == {:error, :invalid_callback}
      refute_receive _
    end

    test "will not start an ExBuffer with an invalid limit" do
      opts = [max_length: -5]

      assert start_ex_buffer(opts) == {:error, :invalid_limit}
      refute_receive _
    end

    test "will not start an ExBuffer with an invalid size callback" do
      opts = [size_callback: nil]

      assert start_ex_buffer(opts) == {:error, :invalid_callback}
      refute_receive _
    end

    test "will flush an ExBuffer on termination" do
      assert {:ok, buffer} = start_ex_buffer()
      assert seed_buffer(buffer) == :ok
      assert GenServer.stop(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end
  end

  describe "chunk/2" do
    test "will correctly chunk an enumerable" do
      opts = [max_length: 3, max_size: 10]
      enum = ["foo", "bar", "baz", "foobar", "barbaz", "foobarbaz"]

      assert {:ok, enum} = ExBuffer.chunk(enum, opts)
      assert Enum.into(enum, []) == [["foo", "bar", "baz"], ["foobar", "barbaz"], ["foobarbaz"]]
    end

    test "will correctly chunk an enumerable with a size callback" do
      opts = [max_size: 8, size_callback: &(byte_size(&1) + 1)]
      enum = ["foo", "bar", "baz"]

      assert {:ok, enum} = ExBuffer.chunk(enum, opts)
      assert Enum.into(enum, []) == [["foo", "bar"], ["baz"]]
    end

    test "will return an error with an invalid callback" do
      opts = [size_callback: fn -> :ok end]
      enum = ["foo", "bar", "baz"]

      assert ExBuffer.chunk(enum, opts) == {:error, :invalid_callback}
    end

    test "will return an error with an invalid limit" do
      opts = [max_length: -5]

      assert ExBuffer.chunk(["foo", "bar", "baz"], opts) == {:error, :invalid_limit}
    end
  end

  describe "chunk!/2" do
    test "will correctly chunk an enumerable" do
      opts = [max_length: 3, max_size: 10]
      enum = ["foo", "bar", "baz", "foobar", "barbaz", "foobarbaz"]
      enum = ExBuffer.chunk!(enum, opts)

      assert Enum.into(enum, []) == [["foo", "bar", "baz"], ["foobar", "barbaz"], ["foobarbaz"]]
    end

    test "will correctly chunk an enumerable with a size callback" do
      opts = [max_size: 8, size_callback: &(byte_size(&1) + 1)]
      enum = ["foo", "bar", "baz"]
      enum = ExBuffer.chunk!(enum, opts)

      assert Enum.into(enum, []) == [["foo", "bar"], ["baz"]]
    end

    test "will raise an error with an invalid callback" do
      opts = [size_callback: fn -> :ok end]
      enum = ["foo", "bar", "baz"]
      fun = fn -> ExBuffer.chunk!(enum, opts) end

      assert_raise ArgumentError, "invalid callback", fun
    end

    test "will raise an error with an invalid limit" do
      opts = [max_length: -5]
      enum = ["foo", "bar", "baz"]
      fun = fn -> ExBuffer.chunk!(enum, opts) end

      assert_raise ArgumentError, "invalid limit", fun
    end
  end

  describe "dump/1" do
    test "will dump the contents of an ExBuffer" do
      assert {:ok, buffer} = start_ex_buffer()
      assert seed_buffer(buffer) == :ok
      assert ExBuffer.dump(buffer) == ["foo", "bar", "baz"]
      assert ExBuffer.length(buffer) == 0
    end
  end

  describe "flush/1" do
    test "will flush an ExBuffer regardless of conditions being met" do
      opts = [max_length: 5, max_size: 20, buffer_timeout: 1_000]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert ExBuffer.flush(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will flush an ExBuffer started from an implementation module" do
      assert {:ok, buffer} = start_test_buffer()
      assert seed_buffer(buffer) == :ok
      assert ExBuffer.flush(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will synchronously flush an ExBuffer" do
      assert {:ok, buffer} = start_ex_buffer()
      assert seed_buffer(buffer) == :ok
      assert ExBuffer.flush(buffer, async: false) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end
  end

  describe "insert/2" do
    test "will correctly insert data into an ExBuffer" do
      assert {:ok, buffer} = start_ex_buffer()
      assert ExBuffer.insert(buffer, "foo") == :ok
      assert ExBuffer.dump(buffer) == ["foo"]
    end

    test "will flush an ExBuffer after hitting max length" do
      opts = [max_length: 3]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will flush an ExBuffer after hitting max size" do
      opts = [max_size: 9]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will flush an ExBuffer with a size callback" do
      opts = [max_size: 12, size_callback: &(byte_size(&1) + 1)]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will flush an ExBuffer after exceeding timeout" do
      opts = [buffer_timeout: 100]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}, 150
    end

    test "will flush an ExBuffer when first condition is met" do
      opts = [max_length: 3, max_size: 10]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will include flush meta when an ExBuffer is flushed" do
      opts = [max_length: 3, flush_meta: "meta"]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], [length: 3, meta: "meta", size: 9]}
    end

    test "will flush an ExBuffer started from an implementation module" do
      opts = [max_size: 12, flush_meta: self()]

      assert {:ok, buffer} = start_test_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end
  end

  describe "length/1" do
    test "will return the length of an ExBuffer" do
      assert {:ok, buffer} = start_ex_buffer()
      assert seed_buffer(buffer) == :ok
      assert ExBuffer.length(buffer) == 3
    end
  end

  describe "next_flush/1" do
    test "will return the time before the next flush" do
      opts = [buffer_timeout: 1_000]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert :timer.sleep(100) == :ok
      assert ExBuffer.next_flush(buffer) < 1_000
    end

    test "will return nil when buffer has no timeout" do
      assert {:ok, buffer} = start_ex_buffer()
      assert is_nil(ExBuffer.next_flush(buffer))
    end
  end

  describe "size/1" do
    test "will return the size of an ExBuffer" do
      assert {:ok, buffer} = start_ex_buffer()
      assert seed_buffer(buffer) == :ok
      assert ExBuffer.size(buffer) == 9
    end

    test "will return the size of an ExBuffer with a size callback" do
      opts = [size_callback: &(byte_size(&1) + 1)]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert ExBuffer.size(buffer) == 12
    end

    test "will return the size of an ExBuffer started from an implementation module" do
      assert {:ok, buffer} = start_test_buffer()
      assert seed_buffer(buffer) == :ok
      assert ExBuffer.size(buffer) == 12
    end
  end
end
