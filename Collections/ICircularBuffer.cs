namespace Allmhuran.Collections
{
   public interface ICircularBuffer<T>
   {
      int Capacity { get; }

      void Enqueue(T item);
      T[] Snapshot();
   }
}