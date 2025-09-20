using System.Diagnostics;
using System.Text;
using ParallelFileProcessor;

namespace TestProject1
{
	[TestClass]
	public sealed class Test1
	{

		[TestMethod]
		[DataRow(100_000, 0)]
		[DataRow(100_000, 2)]
		[DataRow(100_000_000, 0)]
		[DataRow(100_000_000, 4)]
		[DataRow(100_000_000, 8)]
		[DataRow(100_000_000, 12)]
		[DataRow(98_743_123, 12)]
		[DataRow(1_000_000_000, 10)]
		[DataRow(1_000_000_000, 4)]
		public async Task TestMethodParallel(int count, int tasks)
		{
			var tempFile = Path.GetTempFileName();
			File.WriteAllLines(tempFile, Enumerable.Range(0, count).Select(i => i.ToString()), Encoding.UTF8);
			List<int> numbers = new(count);
			try
			{
				var sw = Stopwatch.StartNew();
				var processResult = await ParallelTextFileProcessor.ProcessAsync(
					filePath: tempFile,
					processLine: (batchNumber, line, context) =>
					{
						var value = int.Parse(line);
						context!.Add(value);
						return true;
					},
					contextFactory: async () => await Task.FromResult(new List<int>(count / (ParallelFileProcessor.ParallelTextFileProcessor.RecommendedTasks(tasks)))),
					finalizeTask: async (context) => { lock (numbers) numbers.AddRange(context); await Task.CompletedTask; }
				);
				sw.Stop();
				Console.WriteLine($"Processed {processResult.TotalLines} lines in {sw.ElapsedMilliseconds} ms / {sw.Elapsed}");
				Console.WriteLine($"Process result: {processResult}");

				// NOTE: the lines may be out of order
				//var numbers = lines.Select(l => long.Parse(l)).Order().ToList();
				if (count > 0)
				{
					var min = numbers.Min();
					var max = numbers.Max();
					Assert.AreEqual(0, min);
					Assert.AreEqual(count - 1, max);
				}
				Assert.AreEqual(count, numbers.Count);
				var expectedSum = ((long)count) * (count - 1) / 2;
				Assert.AreEqual(expectedSum, numbers.Select(l => (long)l).Sum());
			}
			finally
			{
				try
				{
					File.Delete(tempFile);
				}
				catch
				{
					// ignore
				}
			}
		}

		string RandomString(int length, char[]? invalidChars = null)
		{
			// use unicode characters
			var sb = new StringBuilder(length);
			for (int i = 0; i < length; i++)
			{
				do
				{
					var c = Random.Shared.Next(0x20, 0xD7FF);
					if (invalidChars != null && Array.IndexOf(invalidChars, (char)c) >= 0)
					{
						continue;
					}

					sb.Append(c);
					break;

				} while (true);
			}
			return sb.ToString();
		}

		[TestMethod]
		[DataRow(100_000, 0)]
		[DataRow(100_000, 2)]
		[DataRow(100_000_000, 0)]
		[DataRow(100_000_000, 4)]
		[DataRow(100_000_000, 8)]
		[DataRow(100_000_000, 12)]
		[DataRow(98_743_123, 12)]

		public async Task TestMethodParallelRandom(int count, int tasks)
		{
			var tempFile = Path.GetTempFileName();
			var invalidChars = new[] { '\r', '\n', ';' };
			var testData = Enumerable.Range(0, count).Select(i => Random.Shared.Next(count)).ToArray();
			File.WriteAllLines(tempFile, testData.Select(i => $"{i};{RandomString(Random.Shared.Next(50), invalidChars)}"), Encoding.UTF8);
			var sw = Stopwatch.StartNew();
			try
			{
				var contextList = new List<List<int>>();
				List<int> factory()
				{
					var dict = new List<int>(count);
					contextList.Add(dict);
					return dict;
				}
				var processResult = await ParallelTextFileProcessor.ProcessAsync<List<int>>(
					filePath: tempFile,
					processLine:
					(batchNumber, line, context) =>
					{
						var idx = line.IndexOf((byte)';');
						var number = int.Parse(line[..idx]);
						context!.Add(number);
						return true;
					},
					contextFactory: async () => await Task.FromResult(factory()),
					numTasks:tasks
				);
				sw.Stop();
				Console.WriteLine($"Processed {processResult.TotalLines} lines in {sw.ElapsedMilliseconds} ms / {sw.Elapsed}");
				Console.WriteLine($"Process result: {processResult}");
				var numbers = contextList.SelectMany(l => l).Order().ToList();

				if (count > 0)
				{
					var min = numbers.Min();
					var max = numbers.Max();
					Assert.AreEqual(testData.Min(), min);
					Assert.AreEqual(testData.Max(), max);
				}
				var expectedSum = testData.Select(i => (long)i).Sum();
				Assert.AreEqual(expectedSum, numbers.Select(l => (long)l).Sum());
				Assert.AreEqual(count, numbers.Count);
			}
			finally
			{
				try
				{
					File.Delete(tempFile);
				}
				catch
				{
					// ignore
				}
			}
		}

		[TestMethod]
		[DataRow("utf-16", true, -1)]
		[DataRow("utf-16BE", true, -1)]
		[DataRow("utf-32", true, -1)]
		[DataRow("utf-32BE", true, -1)]
		[DataRow("utf-8", true, -1)]
		[DataRow("utf-16", false, -1)]
		[DataRow("utf-16BE", false, -1)]
		[DataRow("utf-32", false, -1)]
		[DataRow("utf-32BE", false, -1)]
		[DataRow("utf-8", false, -1)]
		[DataRow("utf-16", true, 99_999)]
		[DataRow("utf-16BE", true, 99_999)]
		[DataRow("utf-32", true, 99_999)]
		[DataRow("utf-32BE", true, 99_999)]
		[DataRow("utf-8", true, 99_999)]
		[DataRow("utf-16", false, 99_999)]
		[DataRow("utf-16BE", false, 99_999)]
		[DataRow("utf-32", false, 99_999)]
		[DataRow("utf-32BE", false, 99_999)]
		[DataRow("utf-8", false, 99_999)]
		public async Task TestMethodParallelEncoding(string encodingName, bool detectEncoding, int chunkSize)
		{
			var count = 1_984_587; // use some non odd, large enough number
			var tempFile = Path.GetTempFileName();
			var encoding = Encoding.GetEncoding(encodingName);
			var testData = Enumerable.Range(0, count).ToArray();
			File.WriteAllLines(tempFile, testData.Select(i => i.ToString()), encoding);

			try
			{
				// test encoding detection
				{
					using var fileStream = new FileStream(tempFile, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan);
					var buffer = new byte[4];
					await fileStream.ReadExactlyAsync(buffer, 0, 4);
					var (detected, toSkip) = ParallelTextFileProcessor.DetectEncoding(buffer);
					Assert.AreEqual(encoding, detected);
					Assert.AreEqual(encoding.Preamble.Length, toSkip);
				}

				var sw = Stopwatch.StartNew();
				var contextList = new List<List<int>>();
				List<int> factory()
				{
					var dict = new List<int>(count);
					contextList.Add(dict);
					return dict;
				}
				var processResult = await ParallelTextFileProcessor.ProcessAsync<List<int>>(
				   filePath: tempFile,
				   processLine: (batchNumber, line, context) =>
				   {
					   var s = encoding.GetString(line);
					   var value = int.Parse(s);
					   context!.Add(value);
					   return true;
				   },
				   contextFactory: async () => await Task.FromResult(factory()),
				   numTasks: -1,
				   encoding: detectEncoding ? null : encoding
			   );
				sw.Stop();
				Console.WriteLine($"Processed {processResult.TotalLines} lines in {sw.ElapsedMilliseconds} ms / {sw.Elapsed}");
				Console.WriteLine($"Process result: {processResult}");
				// note: the items may be out of order
				var numbers = contextList.SelectMany(l => l).Order().ToList();

				if (count > 0)
				{
					var min = numbers.Min();
					var max = numbers.Max();
					Assert.AreEqual(0, min);
					Assert.AreEqual(count - 1, max);
				}
				for (var i = 0; i < Math.Max(numbers.Count, testData.Length); i++)
				{
					if (numbers[i] != testData[i])
					{
						Console.WriteLine($"Mismatch at index {i}: expected {testData[i]}, got {numbers[i]}");
					}
				}
				var expectedSum = testData.Select(i => (long)i).Sum();
				Assert.AreEqual(expectedSum, numbers.Select(l => (long)l).Sum());
				var resultCount = numbers.Count;
				Assert.AreEqual(count, resultCount);
			}
			finally
			{
				try
				{
					File.Delete(tempFile);
				}
				catch
				{
					// ignore
				}
			}

		}

		[TestMethod]
		[DataRow(0)]
		[DataRow(100_000)]
		[DataRow(100_000_000)]
		[DataRow(1_000_000_000)]
		public void TestMethod2(int count)
		{
			var tempFile = Path.GetTempFileName();
			File.WriteAllLines(tempFile, Enumerable.Range(0, count).Select(i => i.ToString()), Encoding.UTF8);
			try
			{

				var totalLines = 0ul;
				var numbers = new long[count]; // new List<long>(count);
				var sw = Stopwatch.StartNew();
				var streamReader = new StreamReader(tempFile, Encoding.UTF8);
				while (!streamReader.EndOfStream)
				{
					var line = streamReader.ReadLine();
					if (line != null)
					{
						var value = long.Parse(line);
						numbers[totalLines] = value;
						totalLines++;
					}
				}
				sw.Stop();
				Console.WriteLine($"Processed {totalLines} lines in {sw.ElapsedMilliseconds} ms / {sw.Elapsed}");

				if (count > 0)
				{
					Assert.AreEqual(0, numbers.Min());
					Assert.AreEqual(count - 1, numbers.Max());
				}
				//var sum = numbers.Sum();
				//Assert.AreEqual(data.Sum(), sum);
				var resultCount = numbers.Length;
				Assert.AreEqual(count, resultCount);
			}
			finally
			{
				try
				{
					File.Delete(tempFile);
				}
				catch
				{
					// ignore
				}

			}
		}

		[TestMethod]
		public void TestEncoding()
		{
			// get list of all supported encodings
			var encodings = Encoding.GetEncodings();
			foreach (var encodingInfo in encodings)
			{
				var encoding = encodingInfo.GetEncoding();
				if (encoding.GetPreamble().Length == 0)
				{
					Console.WriteLine($"Skipping encoding {encoding.EncodingName} as it has no BOM");
					continue;
				}
				var tempFile = Path.GetTempFileName();
				try
				{
					File.WriteAllText(tempFile, "Hallo Welt ☠️🤯🤯", encoding);
					using var fs = File.OpenRead(tempFile);
					var bomData = new byte[4];
					fs.ReadAtLeast(bomData, 4);
					var (detected, bomLength) = ParallelTextFileProcessor.DetectEncoding(bomData);
					var b = ParallelFileProcessor.ParallelTextFileProcessor.GetBytesToSkipForEncoding(encoding, bomData);
					Assert.AreEqual(encoding.GetPreamble().Length, b);
					Assert.AreEqual(encoding, detected);
				}
				finally
				{
					try
					{
						File.Delete(tempFile);
					}
					catch
					{
						// ignore
					}
				}
			}
		}
	}
}