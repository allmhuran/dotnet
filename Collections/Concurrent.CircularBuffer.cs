namespace Allmhuran.Collections.Concurrent
{
   /// <summary>
   /// A circular buffer which allows multiple writers and multiple readers.
   /// </summary>
   /// <remarks>
   /// Uses full locks for simplicity, limiting performance. 
   /// </remarks>
   /// <typeparam name="T"></typeparam>
   public class CircularBuffer<T> : ICircularBuffer<T>
   {
      private readonly T[] _buffer;
      private readonly int _capacity;

      private int _writeIndex;
      private int _count;

      private readonly object _lock = new();

      public int Capacity => _capacity;

      public CircularBuffer(int capacity)
      {
         if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity));

         _capacity = capacity;
         _buffer = new T[_capacity];
      }

      public void Enqueue(T item)
      {
         lock (_lock)
         {
            _buffer[_writeIndex] = item;
            _writeIndex = (_writeIndex + 1) % _capacity;

            if (_count < _capacity)
               _count++;
         }
      }

      public T[] Snapshot()
      {
         lock (_lock)
         {
            var result = new T[_count];
            int head = (_writeIndex - _count + _capacity) % _capacity;

            int idx = head;
            for (int i = 0; i < _count; i++)
            {
               result[i] = _buffer[idx];
               idx++;
               if (idx == _capacity) idx = 0;
            }

            return result;
         }
      }
   }
}
