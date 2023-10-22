opts = [callback: fn _ -> :ok end, max_length: 1_000, max_size: 1_048_576]

with {:ok, pid} <- ExBuffer.start_link(opts) do
  word = Enum.reduce(1..5000, "", fn item, acc -> "#{acc}#{item}" end)
  Benchee.run(%{"test" => fn -> ExBuffer.insert(pid, word) end})
end
