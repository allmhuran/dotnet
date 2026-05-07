namespace Allmhuran.Caching.Concurrent
{
   /// <summary>
   /// Thread safe cache implementing a least-recently-used eviction policy.<br/>
   /// </summary>
   /// <typeparam name="TKey"></typeparam>
   /// <typeparam name="TValue"></typeparam>   
   /// <remarks>
   /// Thread safety is provided by global synchronization locks around shared data.<br/>
   /// This is obviously not ideally performant, but it is easy and safe, and probably fast enough for most use cases!   
   /// The onEvicted callback is invoked outside the lock, so long-running callbacks won't block cache operations.<br/>
   /// </remarks>
   public sealed class LruCache<TKey, TValue> where TKey: notnull
   {

      public delegate Task<IEnumerable<(TKey key, bool found, TValue? value)>> Lookup(IEnumerable<TKey> keys);

      private readonly record struct Entry(TKey key, TValue? value, DateTime? staleAfterUtc)
      {
         public bool isStale => staleAfterUtc.HasValue && staleAfterUtc.Value <= DateTime.UtcNow;
      }

      /// <summary>
      /// Construct a new instance of a cache with least-recently-used eviction policy.
      /// </summary>
      /// <param name="capacity">
      /// The bounded maximum capacity of the cache.<br/>
      /// Once capacity is reached, least-recently-used items will be evicted on subsequent adds.
      /// </param>
      /// <param name="lookup">
      /// Optional delegate that will be invoked to lookup missing items when GetOrLookupAsync is called.<br/>
      /// This allows the cache to be used as a front for a more expensive lookup operation, such as a database or API call.
      /// </param>
      /// <param name="comparer">
      /// Keys must be comparable! Will default to EqualityComparer&lt;TKey&gt;.Default
      /// </param>
      /// <param name="onEvicted">
      /// Optional delegate that will be invoked when an item is evicted.<br/>
      /// Primarily to allow for disposal of unmanaged resources if needed.<br/>
      /// Exceptions thrown by onEvicted are not caught and will propagate to the caller.
      /// </param>
      /// <param name="staleAfter">
      /// Optional timespan after which a cache entry is considered stale and will be evicted on the next access,<br/>
      /// allowing for time-based expiration in addition to LRU eviction.
      /// </param>
      
      /// <exception cref="ArgumentOutOfRangeException"></exception>
      public LruCache
      (
         int capacity,
         IEqualityComparer<TKey>? comparer = null, 
         Lookup? lookup = null,
         Action<TKey, TValue?, EvictionReason>? onEvicted = null,
         TimeSpan? staleAfter = null
      ) 
      {         
         if (capacity <= 0) throw new ArgumentOutOfRangeException("Capacity cannot be negative or zero");
         _capacity = capacity;
         _dictionary = new(capacity, comparer ?? EqualityComparer<TKey>.Default); 
         _lookup = lookup;
         _onEvicted = onEvicted;
         _staleAfter = staleAfter;
      }

      /// <summary>
      /// Encapsulates required logic for adding an item while allowing locking to be handled at a higher level of the call stack,<br/>
      /// enabling atomic addition of single items or whole batches.
      /// </summary>
      /// <param name="key"></param>
      /// <param name="value"></param>
      /// <param name="evicted">If adding an item resulted in the eviction of another item, this will be the evicted item</param>
      private void AddWithoutLock(TKey key, TValue? value, out Entry? evicted)
      {
         evicted = null;

         if (_dictionary.TryGetValue(key, out var existing)) _list.Remove(existing);
         else if (_dictionary.Count >= _capacity)
         {
            evicted = _list.First!.Value;
            _dictionary.Remove(evicted.Value.key);
            _list.RemoveFirst();
         }
         
         var node = new LinkedListNode<Entry>(new (key, value, _staleAfter.HasValue ? DateTime.UtcNow + _staleAfter : null));
         _list.AddLast(node);
         _dictionary[key] = node;
      }
     
      /// <summary>
      /// Atomically add an item to the cache, will invoke the evicted callback if this caused a different item to be evicted.<br/>
      /// </summary>
      /// <remarks>
      /// The eviction callback is invoked outside of the lock to improve concurrency.
      /// </remarks>
      /// <param name="key"></param>
      /// <param name="value"></param>
      public void Add(TKey key, TValue? value)
      {
         Entry? evicted = null;
         lock (_lock) 
         {
            AddWithoutLock(key, value, out evicted);
         }
         if (evicted.HasValue) _onEvicted?.Invoke(evicted.Value.key, evicted.Value.value, EvictionReason.Capacity);
      }

      /// <summary>
      /// Atomically add a set of items to the cache, invoking the evicted callback for any items that were evicted as a result.<br/>
      /// </summary>
      /// <remarks>
      /// The onEvicted callback is invoked outside of the lock to improve concurrency.
      /// </remarks>
      /// <param name="entries"></param>
      public void Add(IEnumerable<(TKey key, TValue? value)> entries)
      {
         // If no onEvicted handler was supplied we dont need to keep track of evictions.
         // But if an eviction handlere was supplied, we don't want to call the handler while the lock is held.
         // Instead store all of the evictions and invoke the handler after the lock is released.
         List<Entry>? evictions = _onEvicted is null ? null : new();

         // we cannot be sure that the client hasn't supplied duplicate keys. If they have, we only need the last value for each key.
         // This is equivalent to getting the same key over and over and replacing the cached entry, but without actually having to do that!
         // We instantiate the distinct entries as a list before locking in order to decrease the time the lock is held.
         var distinctEntries =  entries.GroupBy(e => e.key).Select(g => g.Last()).ToList();

         lock (_lock)
         {
            foreach (var entry in distinctEntries)
            {
               AddWithoutLock(entry.key, entry.value, out var evicted);    
               if (evicted.HasValue && evictions is not null) evictions.Add(evicted.Value);
            }
         }         
         if (evictions is not null)
         { 
            // if evictions is not null then we know an eviction handler exists
            foreach (var entry in evictions) _onEvicted!.Invoke(entry.key, entry.value, EvictionReason.Capacity);  
         }
      }

      /// <summary>
      /// Returns a tuple indicating whether the key was found, and if so, its value.<br/>
      /// If the key was not found, the "found" field will be false, and the value field will be default(TValue).<br/>
      /// </summary>
      /// <remarks>
      /// Each tuple includes the found bool to distinguish between "a result that was not found" and "a result that was found, and its found value happened to be null"<br/>
      /// This method only looks in the cache directly, and will not fall back to the lookup delegate.<br/>
      /// If you want to perform a single item get-or-lookup, provide the single value to GetOrLookupAsync as an enumerable.<br/>
      /// If an entry is found but has passed its expiration date, it will be removed from the cache and returned as if it were not found.<br/>
      /// The onEvicted callback will be invoked if this happens.
      /// </remarks>
      /// <param name="key">the key to find</param>
      public (bool found, TValue? value) Get(TKey key)
      {
         Entry? stale = null;
         (bool, TValue?) result = (false, default);

         lock (_lock) 
         { 
            _getCount++;
            _dictionary.TryGetValue(key, out var node);
            if (node is not null)
            {
               _list.Remove(node);
               if (node.Value.isStale)
               {
                  stale = node.Value;
                  _dictionary.Remove(key);

               }
               else
               {
                  // move the accessed node to the end of the list to mark it as most recently used.
                  _list.AddLast(node);
                  _hitCount++;
                  result = (true, node.Value.value);
               }
            }
         }

         if (stale.HasValue) _onEvicted?.Invoke(stale.Value.key, stale.Value.value, EvictionReason.Stale);
         return result;
      }

      /// <summary>
      /// Attempt to find a set of items in the cache<br/>
      /// If an item is not found, invoke the lookup delegate (if supplied).<br/>
      /// Items found by lookup will be added to the cache.<br/>
      /// </summary>
      /// <remarks>
      /// Lock is released between cache checks and lookup operations.<br/>
      /// This means concurrent requests for the same missing keys may each trigger separate lookups.<br/>
      /// The last result to arrive will be cached.<br/>
      /// This is an acceptable performance/accuracy trade-off to prevent lock contention during I/O.
      /// </remarks>
      /// <param name="keys">The set of keys to find</param>
      public async Task<Dictionary<TKey, (bool found, TValue? value)>> GetOrLookupAsync(IEnumerable<TKey> keys)
      {
         // The client may supply dupiicate keys, but there is no sense in looking up the same key more than once.
         // Therefore we use a hashset to ensure keys are distinct. Since the hashset is instantiated we can also 
         // count the distinct entries and allocate a results dictionary with the exact size required.
         var distinctKeys = new HashSet<TKey>(keys, _dictionary.Comparer);
         var results = new Dictionary<TKey, (bool found, TValue? value)>(distinctKeys.Count);
         var misses = new List<TKey>();
                  
         foreach (var key in distinctKeys)
         {
            var result = Get(key);
            results[key] = result;
            if (!result.found && _lookup is not null) misses.Add(key);                           
         }
         if (misses.Count > 0) 
         {
            // lookup all of the misses, and for those where a result is found via lookup, add it to the cache and update the result.
            var foundLookups = (await _lookup!(misses)).Where(l => l.found).ToList();
            Add(foundLookups.Select(l => (l.key, l.value)));
            foreach (var lookup in foundLookups) results[lookup.key] = (lookup.found, lookup.value);

            // keep track of how many times a lookup succeeded. This is independent of cache hits.
            lock (_lock)
            {
               _lookupHitCount += (ulong)foundLookups.Count;
            }            
         }
         return results;
      }

      /// <summary>
      /// Resets cache hit, lookup hit, and get counters.
      /// </summary>
      public void ResetCounters()
      {
         lock (_lock)
         {
            _hitCount = _lookupHitCount = _getCount = 0;
         }
      }

      /// <summary>
      /// Completely clears the cache, invoking the onEvicted callback (if provided) for every item
      /// </summary>
      public void Clear()
      {
         List<Entry>? evictions = null;
         lock (_lock)
         {            
            if (_onEvicted != null && _list.Count > 0) evictions = new (_list);
            _list.Clear();
            _dictionary.Clear();
            _hitCount = _lookupHitCount = _getCount = 0;
         }         
         if (evictions != null)
         {
            foreach (var entry in evictions) _onEvicted?.Invoke(entry.key, entry.value, EvictionReason.Cleared);
         }
      }
           
      public int Count  { get { lock (_lock) return _dictionary.Count; } }

      public ulong HitCount => Interlocked.Read(ref _hitCount);

      public ulong LookupHitCount => Interlocked.Read(ref _lookupHitCount);

      public ulong GetCount => Interlocked.Read(ref _getCount);

      public int Capacity => _capacity;

      private Action<TKey, TValue?, EvictionReason>? _onEvicted;
      private readonly int _capacity;
      private readonly Dictionary<TKey, LinkedListNode<Entry>> _dictionary;
      private LinkedList<Entry> _list = new();
      private readonly object _lock = new();
      private ulong _hitCount = 0;
      private ulong _lookupHitCount = 0;
      private ulong _getCount = 0;
      private Lookup? _lookup;
      private TimeSpan? _staleAfter;

   }
}
