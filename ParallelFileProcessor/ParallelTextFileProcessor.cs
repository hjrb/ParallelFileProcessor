using System.Buffers;
using System.Text;

namespace ParallelFileProcessor;

/// <summary>
/// class that allows processing a text file using multiple parallel tasks.
/// The file is read in chunks of bytes. the code ensures that every chunk has only full characters.
/// You can either pass the encoding as a parameter if you know it or have it being detected. 
/// The detection will only support encodings with BOM.
/// BOM characters will be skipped automatically.
/// For every line of text (separated by LF (char 10)) a processing routine is called allowing you to process the line.
/// For each task a context object must be constructed by a factory function.
/// For handling context two approaches are possible:
/// 1.) you create the context objects and keep a list of them and process the list of context objects after the file has been processed
/// 2.) you can provide and action method that will be called after a chunk (task) has been processed.
/// </summary>
public class ParallelTextFileProcessor 
{
	/// <summary>
	/// The maximum array size is NOT int.MaxValue but 2147483591
	/// see https://stackoverflow.com/questions/3944320/maximum-length-of-byte
	/// We must use a size that can be divided by 4 to support utf-32 and utf-16 encodings
	/// </summary>
	public const int MaxArraySize = 2147483588; 

	/// <summary>
	/// the result of the processing
	/// </summary>
	/// <param name="ParallelTasks">the number of parallel tasks used</param>
	/// <param name="TotalTasks">the number of total tasks executed, which can be more than parallel tasks</param>
	/// <param name="ChunkSize">the chunk size in bytes processed by each task</param>
	/// <param name="TotalLines">the total number of lines processed</param>
	/// <param name="TotalBytes">the total number of bytes (=file size)</param>
	public sealed record ProcessResult(int ParallelTasks, int TotalTasks, int ChunkSize, ulong TotalLines, long TotalBytes);

	// encoding for utf-32 big endian
	private static readonly Encoding utf32be = Encoding.GetEncoding("utf-32BE");
	// list of supported encodings
	private static readonly Encoding[] supportedEncodings = [Encoding.UTF32, utf32be, Encoding.UTF8, Encoding.Unicode, Encoding.BigEndianUnicode];

	/// <summary>
	/// detect the encoding from the BOM (byte order mark)
	/// </summary>
	/// <param name="bom">a byte array that has to be at least 4 bytes in length</param>
	/// <returns>the detected encoding and the actual number ob BOM bytes used (which can be less) as 4</returns>
	public static (Encoding encoding, int bytesToSkip) DetectEncoding(Span<byte> bom)
	{
		if (bom.Length < 4)
		{
			throw new ArgumentException("BOM array must be at least 4 bytes long", nameof(bom));
		}
		foreach (var enc in supportedEncodings)
		{
			var preamble = enc.GetPreamble();
			if (preamble.Length > 0 && bom[..preamble.Length].SequenceEqual(preamble))
			{
				return (enc, preamble.Length);
			}
		}
		return (Encoding.UTF8, 0); // Default to UTF-8 if no BOM is found
	}

	/// <summary>
	/// load a text file line by line using a defined number of tasks
	/// the number of task is limited to ProcessorCount-1
	/// to process each line a function is called with the taskId, the line as byte span and a context object created by a factory function
	/// </summary>
	/// <param name="filePath">the path to the file to be processed</param>
	/// <param name="contextFactory">a func that creates the context for each thread. The created context will be passed as parameter to the processing method</param>
	/// <param name="processLine">a func that processes the file. the parameters are: 1.) the taskId starting with 0, the current line as <![CDATA[Span<byte>]]>, the context. Returns false to stop the processing.</param>
	/// <param name="numTasks">the number of tasks to use. if value is 0 or negative will use the number of logical processor - 1 which is also the upper limit</param>
	/// <param name="chunkSize">the chunk size to read. if value is 0 or negative will use fileSize/numTasks or MaxArraySize whichever is smaller</param>
	/// <param name="encoding">the encoding to use. if null will try to detect encoding using BOM. if no BOM can be found uses UTF-8 as default.</param>
	/// <param name="finalizeTask">an optional async func that will be called when a task has processed its chunk. The context will be passed as parameter. WARNING: this function may be called concurrently from multiple tasks. Make sure to lock shared variables</param>
	public static async Task<ProcessResult> ProcessAsync<T> 
	(
		string filePath,
		Func<int, Span<byte>, T?, bool> processLine,
		Func<Task<T>>? contextFactory=null,
		Func<T, Task>? finalizeTask = null,
		Encoding? encoding = null,
		int numTasks = -1,
		int chunkSize = -1
	) where T: class
	{
		var fileSize = GetFileLength(filePath);
		numTasks = RecommendedTasks(numTasks);
		chunkSize = RecommendedChunkSize(fileSize: fileSize, numTasks: numTasks, chunkSize: chunkSize);

		// open file stream for sequential reading and shared read access
		using var fileStream = new FileStream(
			filePath,
			FileMode.Open,
			FileAccess.Read,
			FileShare.Read,
			1024 * 1024, // 1 MB buffer size
			FileOptions.SequentialScan | FileOptions.Asynchronous
		);

		// test for BOM supporting encodings
		Span<byte> bomTest = stackalloc byte[4];
		fileStream.ReadExactly(bomTest);
		int bytesToSkip;
		if (encoding == null)
		{
			(encoding, bytesToSkip) = DetectEncoding(bomTest);
		}
		else
		{
			bytesToSkip = GetBytesToSkipForEncoding(encoding, bomTest);
		}
		// initialize file stream index
		var fileStreamIndex = (long)bytesToSkip; // MUST be long

		// we need the byte sequences for new line and carriage return for the encoding
		// TODO: this could be made static
		var newlineBytes = encoding.GetBytes("\n");
		var carriageReturnBytes = encoding.GetBytes("\r");

		// we will pass the task ID so callers can skip lines as needed for taskID==0
		var taskIdx = 0;
		var taskList = new List<Task>(numTasks);
		totalLines = 0ul;
		// loop the file
		while (fileStreamIndex < fileSize)
		{
			_ = fileStream.Seek(fileStreamIndex, SeekOrigin.Begin);
			var fileBuffer = ArrayPool<byte>.Shared.Rent(chunkSize);
			try
			{
				// this will read either the chunkSize or until the end of the stream
				var read = await fileStream.ReadAsync(fileBuffer.AsMemory(0, chunkSize));
				// go back until we find a LF
				var prevNewLine = FindPreviousBytes(fileBuffer, read - 1, read, newlineBytes);
				if (prevNewLine == -1)
				{
					// no new line found - we have to process the whole buffer
					break;
				}
				// we found a new line - we will process until there
				prevNewLine += newlineBytes.Length; // exclude the new line
													// move file index first character after the new line
				// set the pointer to after the next LF
				fileStreamIndex += prevNewLine;

				// remove all finished tasks before adding new tasks
				_ = taskList.RemoveAll(t => t.IsCompleted);

				// wait until a new task can be created
				if (taskList.Count >= numTasks)
				{
					// wait for any task to complete
					await Task.WhenAny([.. taskList]);
				}

				// create a task to process the chunk
				taskIdx++;
				taskList.Add(
					CreateTaskForChunk(processLine, contextFactory, finalizeTask, newlineBytes, carriageReturnBytes, fileBuffer, read, taskIdx)
				);
			}
			catch
			{
				ArrayPool<byte>.Shared.Return(fileBuffer);
				throw;
			}
		}
		// wait for all tasks to finish
		await Task.WhenAll(taskList);
		return new ProcessResult(numTasks, taskIdx, chunkSize, totalLines, fileSize);
	}

	private static ulong totalLines = 0ul;

	private static Task CreateTaskForChunk<T>(
		Func<int, Span<byte>, T?, bool> processLine,
		Func<Task<T>>? contextFactory,
		Func<T, Task>? finalizeTask,
		byte[] newlineBytes,
		byte[] carriageReturnBytes,
		byte[] fileBuffer,
		int fileBufferLength,
		int taskIdx
	)
	{
		return Task.Run(async () =>
		{
			// create context
			T? context = contextFactory != null ? await contextFactory() : default;
			try
			{
				var taskBuffer = fileBuffer.AsSpan(0, fileBufferLength);
				var bufferIdx = 0;
				do
				{
					// find next new line
					var nextNewLine = FindNextBytes(taskBuffer, bufferIdx, newlineBytes); // 		if (Encoding.UTF32.Preamble.SequenceEqual(bom.AsSpan(0, 4)))

					if (nextNewLine == -1)
					{
						// No more new lines in this chunk - done
						break;
					}
					var l = nextNewLine - bufferIdx;
					// check for carriage return before new line
					var previousCarriageReturn = FindPreviousBytes(taskBuffer, nextNewLine, carriageReturnBytes.Length, carriageReturnBytes);
					if (previousCarriageReturn >= 0)
					{
						l -= carriageReturnBytes.Length;
					}
					// get the line slice
					var slice = taskBuffer.Slice(bufferIdx, l);
					_ = Interlocked.Increment(ref totalLines);
					// process the line
					if (!processLine(taskIdx, slice, context))
					{
						break; // stop processing
					}
					// move to beginning of next line
					bufferIdx = nextNewLine + newlineBytes.Length;
				} while (bufferIdx < taskBuffer.Length);
				if (finalizeTask != null && context is not null)
				{
					await finalizeTask(context);
				}
			}
			finally
			{
				ArrayPool<byte>.Shared.Return(fileBuffer);
			}
		});
	}

	/// <summary>
	/// check if the given encoding matches the BOM in the given byte array and return the number of bytes to skip
	/// </summary>
	/// <param name="encoding"></param>
	/// <param name="bomTest"></param>
	/// <returns></returns>
	public static int GetBytesToSkipForEncoding(Encoding encoding, Span<byte> bomTest)
	{
		int bytesToSkip = 0;
		foreach (var item in supportedEncodings)
		{
			if (item.EncodingName.Equals(encoding.EncodingName) &&  // list of support supported encodings
				bomTest[..item.GetPreamble().Length].SequenceEqual(item.GetPreamble()))
			{
				bytesToSkip = item.GetPreamble().Length;
				break;
			}
		}
		return bytesToSkip;
	}

	private static int FindNextBytes(Span<byte> buffer, int startOffset, byte[] bytesToFind)
	{
		if (buffer.Length < bytesToFind.Length)
		{
			return -1;
		}

		var max = buffer.Length - bytesToFind.Length; 
		for (var i = startOffset; i <= max; i++)
		{
			if (buffer.Slice(i, bytesToFind.Length).SequenceEqual(bytesToFind))
			{
				return i; // Return the found position
			}
		}
		return -1; // Not found
	}

	private static int FindPreviousBytes(Span<byte> buffer, int startOffset, int maxLength, byte[] bytesToFind)
	{
		if (buffer.Length < bytesToFind.Length)
		{
			return -1;
		}

		var min = Math.Max(0, startOffset - maxLength);
		var lastPossible = buffer.Length - bytesToFind.Length;
		var searchStart = Math.Min(startOffset, lastPossible);
		for (var i = searchStart; i >= min; i--)
		{
			if (buffer.Slice(i, bytesToFind.Length).SequenceEqual(bytesToFind))
			{
				return i; // Return the found position
			}
		}
		return -1; // Not found
	}


	/// <summary>
	/// return the previous multiple of 4 for the given value
	/// </summary>
	/// <param name="value">a positive value</param>
	/// <returns></returns> 
	public static int PreviousMultipleOf4(int value) =>
		value <0 ? throw new ArgumentException("value must be positive or zero", nameof(value)) : value - (value % 4);

	/// <summary>
	/// return the recommended chunk size for reading the file
	/// </summary>
	/// <param name="fileSize">the file size</param>
	/// <param name="numTasks">the number of tasks being used. This must be greater or equal 1</param>
	/// <param name="chunkSize">the chunk size you think is good or a value less or equal 0 </param>
	/// <returns></returns>
	public static int RecommendedChunkSize(long fileSize, int numTasks, int chunkSize = -1) =>
		 PreviousMultipleOf4(
			fileSize < 0
			? throw new ArgumentOutOfRangeException(nameof(fileSize), "fileSize must be greater or equal 0")
			: numTasks < 1
			? throw new ArgumentOutOfRangeException(nameof(numTasks), "numTasks must be greater or equal 1")
			: chunkSize < 0 ? (int)long.Min(MaxArraySize, fileSize / numTasks) : Math.Min(MaxArraySize, chunkSize)
		);

	/// <summary>
	/// returns then number of tasks. if the input is too large will be limited to Environment.ProcessorCount - 1 
	/// </summary>
	/// <param name="numTasks"></param>
	/// <returns></returns>
	public static int RecommendedTasks(int numTasks) =>
		numTasks < 1 ? Environment.ProcessorCount : Math.Min(numTasks, Environment.ProcessorCount);

	/// <summary>
	/// return the length of a file in bytes as long
	/// </summary>
	/// <param name="filePath">the path to the file</param>
	/// <returns></returns>
	public static long GetFileLength(string filePath) =>
		 new FileInfo(filePath).Length;
}

