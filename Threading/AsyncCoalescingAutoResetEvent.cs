using System.Threading;

namespace Allmhuran.Threading
{
   /// <summary>
   /// Provides functionality similar to AutoResetEvent, but with async/await support.<br/>
   /// </summary>
   /// <remarks>
   /// Multiple calls to <see cref="Set"/> will be coalesced,<br/>
   /// making this class unsuitable in scenarios where signals are very frequent and must each be individually honored.
   /// </remarks>
   public sealed class AsyncCoalescingAutoResetEvent
   {
       private readonly SemaphoreSlim _semaphore = new(0, 1);

       public Task WaitAsync(CancellationToken token) => _semaphore.WaitAsync(token);

       public void Set()
       {
           try
           {
               _semaphore.Release();
           }
           catch (SemaphoreFullException)
           {
               // Already signaled — collapse multiple Set() calls
           }
       }
   }
}
