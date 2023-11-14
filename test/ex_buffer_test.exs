defmodule ExBufferTest do
  use ExUnit.Case, async: true
  # doctest ExBuffer

  import ExBuffer.Helpers

  # TODO(Gordon) - update "describe" for tests

  setup %{test_type: test_type} do
    if test_type == :doctest do
      opts = [name: :buffer, flush_callback: fn _, _ -> :ok end, buffer_timeout: 5_000]
      if start_ex_buffer(opts) == {:ok, :buffer}, do: :ok
    else
      :ok
    end
  end

  describe "start_link/2" do
    test "will start an unpartitioned ExBuffer" do
      assert start_ex_buffer() == {:ok, ExBuffer}
    end

    test "will correctly name an unpartitioned ExBuffer" do
      opts = [name: :ex_buffer]

      assert start_ex_buffer(opts) == {:ok, :ex_buffer}
    end

    test "will start a partitioned ExBuffer" do
      opts = [partitions: 2]

      assert start_ex_buffer(opts) == {:ok, ExBuffer}
    end

    test "will correctly name a partitioned ExBuffer" do
      opts = [name: :ex_buffer, partitions: 2]

      assert start_ex_buffer(opts) == {:ok, :ex_buffer}
    end

    test "will correctly start an ExBuffer from an implementation module" do
      assert start_test_buffer() == {:ok, ExBuffer.TestBuffer}
    end

    test "will correctly name an ExBuffer started from an implementation module" do
      opts = [name: :ex_buffer]

      assert start_test_buffer(opts) == {:ok, :ex_buffer}
    end

    test "will jitter the limits of an ExBuffer" do
      opts = [jitter_rate: 0.05, max_size: 10_000, partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert {:ok, [%{max_size: limit_1}, %{max_size: limit_2}]} = ExBuffer.info(buffer)
      assert limit_1 != limit_2
    end

    test "will not start with an invalid flush callback" do
      opts = [flush_callback: nil]

      assert start_ex_buffer(opts) == {:error, :invalid_callback}
    end

    test "will not start with an invalid size callback" do
      opts = [size_callback: fn _, _ -> :ok end]

      assert start_ex_buffer(opts) == {:error, :invalid_callback}
    end

    test "will not start with an invalid limit" do
      opts = [buffer_timeout: -5]

      assert start_ex_buffer(opts) == {:error, :invalid_limit}
    end

    test "will not start with an invalid partition count" do
      opts = [partitions: -2]

      assert start_ex_buffer(opts) == {:error, :invalid_partitions}
    end

    test "will not start with an invalid partitioner" do
      opts = [partitioner: :fake_partitioner]

      assert start_ex_buffer(opts) == {:error, :invalid_partitioner}
    end

    test "will not start with an invalid jitter rate" do
      opts = [jitter_rate: 3.14]

      assert start_ex_buffer(opts) == {:error, :invalid_jitter}
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
    test "will dump an unpartitioned ExBuffer" do
      assert {:ok, buffer} = start_ex_buffer()
      assert seed_buffer(buffer) == :ok
      assert ExBuffer.dump(buffer) == {:ok, ["foo", "bar", "baz"]}
      assert {:ok, [%{length: 0}]} = ExBuffer.info(buffer)
    end

    test "will dump a partitioned ExBuffer" do
      opts = [partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert ExBuffer.dump(buffer) == {:ok, ["foo", "baz", "bar"]}
      assert {:ok, [%{length: 0}, %{length: 0}]} = ExBuffer.info(buffer)
    end

    test "will dump a specific ExBuffer partition" do
      opts = [partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert ExBuffer.dump(buffer, partition: 0) == {:ok, ["foo", "baz"]}
      assert {:ok, [%{length: 0}]} = ExBuffer.info(buffer, partition: 0)
    end

    test "will return an error with an invalid buffer" do
      assert ExBuffer.dump(:fake_buffer) == {:error, :not_found}
    end

    test "will return an error with an invalid partition" do
      assert {:ok, buffer} = start_ex_buffer()
      assert ExBuffer.dump(buffer, partition: -1) == {:error, :invalid_partition}
    end
  end

  describe "flush/1" do
    test "will flush an unpartitioned ExBuffer" do
      assert {:ok, buffer} = start_ex_buffer()
      assert seed_buffer(buffer) == :ok
      assert ExBuffer.flush(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will flush a partitioned ExBuffer" do
      opts = [partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert ExBuffer.flush(buffer) == :ok
      assert_receive {^buffer, ["foo", "baz"], _}
      assert_receive {^buffer, ["bar"], _}
    end

    test "will flush a specific ExBuffer partition" do
      opts = [partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert ExBuffer.flush(buffer, partition: 0) == :ok
      assert_receive {^buffer, ["foo", "baz"], _}
      refute_receive _
    end

    test "will flush an ExBuffer started from an implementation module" do
      assert {:ok, buffer} = start_test_buffer()
      assert seed_buffer(buffer) == :ok
      assert ExBuffer.flush(buffer) == :ok
      assert_receive {:impl_mod, ["foo", "bar", "baz"], _}
    end

    test "will synchronously flush an ExBuffer" do
      assert {:ok, buffer} = start_ex_buffer()
      assert seed_buffer(buffer) == :ok
      assert ExBuffer.flush(buffer, async: false) == :ok
      assert_received {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will include flush meta" do
      opts = [flush_meta: "meta"]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert ExBuffer.flush(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], flush_opts}
      assert Keyword.get(flush_opts, :meta) == "meta"
    end

    test "will return an error with an invalid buffer" do
      assert ExBuffer.flush(:fake_buffer) == {:error, :not_found}
    end

    test "will return an error with an invalid partition" do
      assert {:ok, buffer} = start_ex_buffer()
      assert ExBuffer.flush(buffer, partition: -1) == {:error, :invalid_partition}
    end
  end

  describe "info/2" do
    test "will return info for an unpartitioned ExBuffer" do
      assert {:ok, buffer} = start_ex_buffer()
      assert seed_buffer(buffer) == :ok
      assert {:ok, [%{length: 3}]} = ExBuffer.info(buffer)
    end

    test "will return info for a partitioned ExBuffer" do
      opts = [partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert {:ok, [%{length: 2}, %{length: 1}]} = ExBuffer.info(buffer)
    end

    test "will return info for a specific ExBuffer partition" do
      opts = [partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert {:ok, [%{length: 2}]} = ExBuffer.info(buffer, partition: 0)
    end

    test "will return info for an ExBuffer with a size callback" do
      opts = [size_callback: &(byte_size(&1) + 1)]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert {:ok, [%{size: 12}]} = ExBuffer.info(buffer)
    end

    test "will return info for an ExBuffer started from an implementation module" do
      assert {:ok, buffer} = start_test_buffer()
      assert seed_buffer(buffer) == :ok
      assert {:ok, [%{size: 12}]} = ExBuffer.info(buffer)
    end

    test "will include next flush when applicable" do
      opts = [buffer_timeout: 1_000]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert {:ok, [%{next_flush: next_flush}]} = ExBuffer.info(buffer)
      refute is_nil(next_flush)
    end

    test "will return an error with an invalid buffer" do
      assert ExBuffer.info(:fake_buffer) == {:error, :not_found}
    end

    test "will return an error with an invalid partition" do
      assert {:ok, buffer} = start_ex_buffer()
      assert ExBuffer.info(buffer, partition: -1) == {:error, :invalid_partition}
    end
  end

  describe "insert/2" do
    test "will insert items into an unpartitioned ExBuffer" do
      assert {:ok, buffer} = start_ex_buffer()
      assert ExBuffer.insert(buffer, "foo") == :ok
      assert ExBuffer.dump(buffer) == {:ok, ["foo"]}
    end

    test "will insert items into a partitioned ExBuffer" do
      opts = [partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert ExBuffer.insert(buffer, "foo") == :ok
      assert ExBuffer.insert(buffer, "bar") == :ok
      assert ExBuffer.dump(buffer, partition: 0) == {:ok, ["foo"]}
      assert ExBuffer.dump(buffer, partition: 1) == {:ok, ["bar"]}
    end

    test "will insert items into a partitioned ExBuffer with a random partitioner" do
      opts = [partitioner: :random, partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
    end

    test "will flush an ExBuffer based on a length condition" do
      opts = [max_length: 3]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will flush an ExBuffer based on a size condition" do
      opts = [max_size: 9]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will flush an ExBuffer with a size callback based on a size condition" do
      opts = [max_size: 12, size_callback: &(byte_size(&1) + 1)]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will flush an ExBuffer based on a time condition" do
      opts = [buffer_timeout: 50]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok

      :timer.sleep(50)

      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will flush an ExBuffer once the first condition is met" do
      opts = [max_length: 5, max_size: 9]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], _}
    end

    test "will flush a ExBuffer partitions independently" do
      opts = [max_length: 2, partitions: 2]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "baz"], _}
      assert {:ok, [%{length: 1}]} = ExBuffer.info(buffer, partition: 1)
    end

    test "will flush an ExBuffer started from an implementation module" do
      opts = [max_length: 3]

      assert {:ok, buffer} = start_test_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {:impl_mod, ["foo", "bar", "baz"], _}
    end

    test "will include flush meta when flushed" do
      opts = [flush_meta: "meta", max_length: 3]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert seed_buffer(buffer) == :ok
      assert_receive {^buffer, ["foo", "bar", "baz"], flush_opts}
      assert Keyword.get(flush_opts, :meta) == "meta"
    end

    test "will return an error with an invalid buffer" do
      assert ExBuffer.insert(:fake_buffer, "foo") == {:error, :not_found}
    end
  end

  describe "insert_batch/2" do
    test "will insert a batch of items into an unpartitioned ExBuffer" do
      items = ["foo", "bar", "baz"]

      assert {:ok, buffer} = start_ex_buffer()
      assert ExBuffer.insert_batch(buffer, items) == :ok
      assert ExBuffer.dump(buffer) == {:ok, ["foo", "bar", "baz"]}
    end

    test "will insert a batch of items into a partitioned ExBuffer" do
      opts = [partitions: 2]
      items = ["foo", "bar", "baz"]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert ExBuffer.insert_batch(buffer, items) == :ok
      assert ExBuffer.dump(buffer, partition: 0) == {:ok, ["foo", "bar", "baz"]}
    end

    test "will flush an ExBuffer while inserting a batch of items" do
      opts = [max_length: 2]
      items = ["foo", "bar", "baz"]

      assert {:ok, buffer} = start_ex_buffer(opts)
      assert ExBuffer.insert_batch(buffer, items) == :ok
      assert_receive {^buffer, ["foo", "bar"], _}
      assert ExBuffer.dump(buffer) == {:ok, ["baz"]}
    end
  end
end
