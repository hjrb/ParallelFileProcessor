# ParallelFileProcessor

High?performance, allocation–aware, parallel line processor for very large text files.

Processes a file by splitting it into byte chunks (aligned so multi?byte characters are not broken) and spinning up multiple tasks that each parse lines (LF separated, optional CR) directly from pooled byte buffers. Supports UTF?8, UTF?16 LE/BE, UTF?32 LE/BE via BOM detection (or explicit encoding). No per?line string allocations unless you create them yourself.

## Features
- Parallel chunk processing with bounded task count
- Zero copy line slices (`Span<byte>`) – caller decides if/when to decode
- BOM detection (UTF?8 / UTF?16 LE / UTF?16 BE / UTF?32 LE / UTF?32 BE)
- Optional user context per task (created via async factory)
- Optional finalize callback per task (async) for aggregation / flushing
- Adaptive (or user supplied) chunk sizing; always 4?byte aligned
- Manual control to stop early by returning `false` from the line callback
- Uses `ArrayPool<byte>` to minimize GC pressure

## Core Type
`ParallelTextFileProcessor` with main API:
```
Task<ProcessResult> ProcessAsync<T>(
    string filePath,
    Func<int, Span<byte>, T?, bool> processLine,
    Func<Task<T>>? contextFactory = null,
    Func<T, Task>? finalizeTask = null,
    Encoding? encoding = null,
    int numTasks = -1,
    int chunkSize = -1) where T : class
```

`ProcessResult` fields:
- ParallelTasks: actual parallelism used
- TotalTasks: total chunks scheduled (can exceed parallelism)
- ChunkSize: bytes per chunk (4?byte aligned)
- TotalLines: total processed lines
- TotalBytes: file size

## When To Use
- Log analytics over multi?GB log files
- ETL pre?processing (filtering, counting, bucketing)
- Building indexes / statistics from raw text
- Fast scanning for patterns before deeper parsing

## Quick Start
```bash
dotnet add package ParallelFileProcessor
```

### Minimal Example (count lines containing an error token)
```csharp
using ParallelFileProcessor;
using System.Text;

var targetFile = @"/data/huge.log";
var needle = Encoding.UTF8.GetBytes("ERROR");
long errorCount = 0;

var result = await ParallelTextFileProcessor.ProcessAsync<object>(
    filePath: targetFile,
    processLine: (taskId, lineBytes, _ctx) =>
    {
        // simple contains check on raw bytes
        if (lineBytes.IndexOf(needle) >= 0)
        {
            Interlocked.Increment(ref errorCount);
        }
        return true; // continue
    }
);
Console.WriteLine(result); // shows stats
Console.WriteLine($"Lines with ERROR: {errorCount}; Total lines: {result.TotalLines}");
```

### Using Context + Finalization
```csharp
class TaskContext
{
    public int LocalMatches;
}

var utf8 = Encoding.UTF8;
var pattern = utf8.GetBytes("WARN");
int globalMatches = 0;
object locker = new();

var res = await ParallelTextFileProcessor.ProcessAsync<TaskContext>(
    filePath: "big.log",
    contextFactory: async () => new TaskContext(), // could await async init
    processLine: (taskId, line, ctx) =>
    {
        if (line.IndexOf(pattern) >= 0)
            ctx!.LocalMatches++;
        return true;
    },
    finalizeTask: async ctx =>
    {
        lock (locker) globalMatches += ctx.LocalMatches;
        await Task.CompletedTask;
    },
    encoding: null,      // auto detect via BOM else UTF?8
    numTasks: -1,        // auto (Environment.ProcessorCount)
    chunkSize: -1        // auto (fileSize/numTasks bounded)
);

Console.WriteLine($"Total WARN lines: {globalMatches}");
```

### Using context
```csharp
var count = 1_984_587; // use some non odd, large enough number
var tempFile = Path.GetTempFileName();
var encoding = Encoding.GetEncoding("utf-16");
var testData = Enumerable.Range(0, count).ToArray();
using var cts = new CancellationTokenSource();
// write sequential integers, one per line
await File.WriteAllLinesAsync(tempFile, testData.Select(i => i.ToString()), encoding, cts.Token);

// global context list to hold all task contexts - must be synchronized!!
var contextList = new List<List<int>>();
// factory to create a new context for each task
List<int> factory()
{
    var list = new List<int>(count);
    lock (contextList) // MUST lock as this may be called concurrently
        contextList.Add(list);
    return list;
}
// process the file
var processResult = await ParallelTextFileProcessor.ProcessAsync<List<int>>(
    filePath: tempFile,
    processLine: (batchNumber, line, context) =>
    {
        var s = encoding.GetString(line); // this could be avoided if we processed bytes directly
        var value = int.Parse(s);
        context!.Add(value); // this doese not need locking as each context is task-local
        return true;
    },
    contextFactory: async () => await Task.FromResult(factory()),
    numTasks: -1,
    encoding: encoding,
    chunkSize: -1 // auto size
);
Console.WriteLine($"Processed {processResult.TotalLines} lines");
Console.WriteLine($"Process result: {processResult}");
// note: the items may be out of order
var numbers = contextList.SelectMany(l => l).Order().ToList();
// to more stuff with the numbers...
```
### Early Stop
Return `false` in `processLine` when you have found enough matches.

### Accessing Text (Decoding)
To convert a `Span<byte>` line to string only when needed:
```csharp
var text = utf8.GetString(lineBytes);
```
Avoid decoding every line if you can operate on bytes directly.

## Encoding Notes
- If no `encoding` supplied: first 4 bytes are inspected for known BOMs; default = UTF?8 w/o BOM
- Lines are split on `\n` (LF). A preceding `\r` (CR) for CRLF endings is trimmed
- Chunk boundaries are moved backward to the previous LF so multi?byte chars are never split

## Sizing & Performance
- Default chunk size ? fileSize / numTasks (capped by `MaxArraySize`); always multiple of 4
- Tune `chunkSize` manually for scenarios with extremely skewed line lengths
- Larger chunks = fewer tasks, lower scheduling overhead
- Too large chunks can reduce parallelism if lines are sparse near boundaries

## Thread Safety
- `processLine` runs concurrently on multiple tasks
- `finalizeTask` may run concurrently for different contexts
- Use synchronization for shared state (`Interlocked`, `lock`, concurrent collections)

## Limitations / TODO
- Only BOM?detectable encodings supported
- No built?in backpressure if `finalizeTask` is slow
- Does not presently expose cancellation token
- No direct support for multi?line record formats
- Could expose pluggable line delimiter logic / multi?char terminators

## Error Handling
Exceptions inside a task propagate via `Task.WhenAll` and will fault the overall `ProcessAsync` call (unpooled buffers are still returned in `finally`).

## API Helpers
- `DetectEncoding(Span<byte>)`
- `GetBytesToSkipForEncoding(Encoding, Span<byte>)`
- `RecommendedTasks(int)`
- `RecommendedChunkSize(long, int, int)`
- `PreviousMultipleOf4(int)`
- `GetFileLength(string)`

## Return Semantics
`TotalTasks` can exceed `ParallelTasks` because the file may require more chunks than the number of concurrently active tasks.

## Benchmarking (Suggested)
Use `BenchmarkDotNet` or .NET Trace/PerfView to measure real workloads (not included yet).

## Contributing
Issues + PRs welcome: tests, cancellation support, custom delimiter strategies, streaming decode helpers.

## License
MIT (add SPDX header / file if not already present).

## Disclaimer
Target framework set to `.NET 10` (preview at time of writing). Adjust if your environment lacks latest SDK.