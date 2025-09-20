using BenchmarkDotNet.Attributes;

using nietras.SeparatedValues;

using ParallelFileProcessor;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

using Tools;

namespace PerformanceTest
{
    [MemoryDiagnoser]
    public class TestPerformance
    {
        private string tempFile = string.Empty;
        [Params(1_000, 100_000, 1_000_000, 10_000_000_000)]
        public long RowCount { get; set; }
        [GlobalSetup]
        public void Setup()
        {
            tempFile = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName() + ".txt");
            var invalidChars = new[] { '\r', '\n', ';' };
            using var streamWriter = new StreamWriter(tempFile, false, Encoding.UTF8);
            for (var i = 0L; i < RowCount; i++)
            {
                var s=$"{i};{StaticTools.RandomString(Random.Shared.Next(50), invalidChars)}";
                streamWriter.WriteLine(s);
            }
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            try
            {
                if (File.Exists(tempFile))
                {
                    File.Delete(tempFile);
                }
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        [Benchmark]
        public async Task UseParallelTextFileProcessorAsync()
        {
            var contextList = new List<List<int>>();
            List<int> factory()
            {
                var dict = new List<int>();
                contextList.Add(dict);
                return dict;
            }
            _ = await ParallelTextFileProcessor.ProcessAsync<List<int>>(
                filePath: tempFile,
                processLine:
                (batchNumber, line, context) =>
                {
                    var idx = line.IndexOf((byte)';');
                    var number = int.Parse(line[..idx]);
                    context!.Add(number);
                    return true;
                },
                contextFactory: async () => await Task.FromResult(factory())
            );
        }

        [Benchmark]
        public async Task UseParallelTextFileProcessorNoContextFactoryAsync()
        {
            var allLists = new List<int>();
            _ = await ParallelTextFileProcessor.ProcessAsync<List<int>>(
                filePath: tempFile,
                contextFactory: async () => await Task.FromResult(new List<int>()),
                processLine:
                (batchNumber, line, context) =>
                {
                    var idx = line.IndexOf((byte)';');
                    var number = int.Parse(line[..idx]);
                    context!.Add(number);
                    return true;
                },
                finalizeTask: async (list) => { lock (allLists) allLists.AddRange(list); await Task.CompletedTask; }
            );
        }

        [Benchmark]
        public void UseSep()
        {
            var numberList = new List<int>();
            var rows = Sep.New(';').Reader().FromFile(tempFile);
            foreach (var row in rows)
            {
                var number = row[0].Parse<int>();
                numberList.Add(number);
            }
        }

    }
}
