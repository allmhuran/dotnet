using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Allmhuran.Extensions
{
   public static partial class AsyncEnumerableExtensions
   {
      /// <summary>
      /// Allows us to write a sentinal into the channel when it's time to flush.
      /// </summary>
      private readonly record struct Item<T>(T? Value, bool Flush);

      /// <summary>
      /// Given an IAsyncEnumerable&lt;T&gt;, creates batches of items with a maximum batch size and a maximum delay between batches.
      /// </summary>
      /// <remarks>
      /// A consumer will not wait longer than the specified maximum delay if data is available.<br/>
      /// The interval between batches may be less than the specified max delay (or even zero) if data is arriving rapidly,<br/>
      /// and the interval may also be greater than the specified delay if no data is available.<br/>
      /// The implementation wil be quiesced (await) when no data is available.<br/>
      /// Memory consumption may be up to 2x the batch size when data is arriving rapidly.
      /// </remarks>
      /// <typeparam name="T"></typeparam>
      /// <param name="source">The source IAsyncEnumerable to be processed.</param>
      /// <param name="batchSize">The maximum number of items in each batch.</param>
      /// <param name="maxDelay">The maximum delay before a batch is released.</param>
      /// <param name="cancellationToken">A token to cancel the operation.</param>
      /// <returns>An IAsyncEnumerable of batches, each batch being a List&lt;T&gt;</returns>
      /// <exception cref="ArgumentNullException"></exception>
      /// <exception cref="ArgumentOutOfRangeException"></exception>
      /// <exception cref="OperationCanceledException"></exception>
      public static async IAsyncEnumerable<List<T>> ChunkWithTimeout<T>
      (
         this IAsyncEnumerable<T> source,
         int batchSize,
         TimeSpan maxDelay,
         [System.Runtime.CompilerServices.EnumeratorCancellation]
         CancellationToken cancellationToken = default
      )
      {
         if (source is null) throw new ArgumentNullException(nameof(source));
         if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize));
         if (maxDelay <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(maxDelay));

         // Capacity of this channel is batchSize + 1 to make room for a sentinel coming from the timer while still providing backpressure.
         // Even so, if data is coming in very quickly we might not be able to write the sentinel.
         // But if data is coming in very quickly then we are releasing full batches quickly anyway.
         // We might have to wait one extra timer tick for a remainder batch at the end of a long stream of fast data,
         // but that's an acceptable and very rare edge case.
         var input = Channel.CreateBounded<Item<T>>
         (
            new BoundedChannelOptions(batchSize + 1) { SingleReader = true, SingleWriter = false }
         );

         var timer = new Timer(_ => input.Writer.TryWrite(new Item<T>(default, Flush: true)), null, Timeout.Infinite, Timeout.Infinite);

         var inputProcessor = Task.Run(async () =>
         {
            try
            {
               await foreach (var item in source.WithCancellation(cancellationToken))
               {
                  await input.Writer.WriteAsync(new Item<T>(item, Flush: false), cancellationToken);
               }
            }
            finally
            {
               input.Writer.Complete();
            }
         });

         var batch = new List<T>(batchSize);
         var sw = new Stopwatch();

         try
         {
            while (await input.Reader.WaitToReadAsync(cancellationToken))
            {
               while (input.Reader.TryRead(out var item))
               {
                  if (item.Flush)
                  {
                     timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                     if (batch.Count > 0)
                     {
                        yield return batch;
                        sw.Restart();
                        batch = new List<T>(batchSize);
                     }
                  }
                  else
                  {
                     if (batch.Count == 0)
                     {
                        var remaining = maxDelay.TotalMilliseconds - sw.ElapsedMilliseconds;
                        timer.Change(remaining <= 0 ? TimeSpan.Zero : TimeSpan.FromMilliseconds(remaining), Timeout.InfiniteTimeSpan);
                     }
                     batch.Add(item.Value!);
                     if (batch.Count == batchSize)
                     {
                        yield return batch;
                        sw.Restart();
                        batch = new List<T>(batchSize);
                     }
                  }
               }
            }
            if (batch.Count > 0) yield return batch;            
         }
         finally 
         { 
            timer.Dispose(); 
            try { await inputProcessor; } catch (OperationCanceledException) { }
         }
      }
   }
}
