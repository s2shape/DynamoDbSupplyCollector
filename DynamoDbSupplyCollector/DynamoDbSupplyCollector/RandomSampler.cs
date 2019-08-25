using System;
using System.Collections.Generic;
using System.Text;

namespace DynamoDbSupplyCollector
{
    public class RandomSampler
    {
        private readonly int _sampleSize;
        private readonly long _itemsCount;
        private readonly Random _random;
        private readonly double _minPercentage;
        const int DAFAULT_MIN_PERCENTAGE = 10;

        public RandomSampler(int sampleSize, long itemsCount, double minPercentage = 0)
        {
            _sampleSize = sampleSize;
            _itemsCount = itemsCount;
            _random = new Random();
            _minPercentage = minPercentage == 0 ? DAFAULT_MIN_PERCENTAGE : minPercentage;
        }

        public List<T> Random<T>(Func<long, List<T>> getData)
        {
            var (data, percentage) = GetData(getData);

            if (data.Count <= _sampleSize)
                return data;

            var sampleCollection = new List<T>();
            int currentIdx = 0;
            while (currentIdx < data.Count)
            {
                var randomLessThan100 = _random.Next(100);
                var currentItem = data[currentIdx];

                if (randomLessThan100 <= percentage &&
                    !sampleCollection.Contains(currentItem)) // to make sure we don't add one item twice in case we go over the collection not for the first time
                {
                    sampleCollection.Add(currentItem);
                }

                if (sampleCollection.Count == _sampleSize)
                    break;

                ++currentIdx;

                if (currentIdx == data.Count)
                    currentIdx = 0; // reset index to go over the collection again to have enough items in the result
            }

            return sampleCollection;
        }

        private (List<T>, double) GetData<T>(Func<long, List<T>> getData)
        {
            var percentage = (double)_sampleSize / _itemsCount * 100;

            if (percentage >= _minPercentage)
            {
                var data = getData(_itemsCount);

                return (data, percentage);
            }
            else
            {
                var count = _sampleSize * _minPercentage;
                var data = getData((int)count);

                return (data, _minPercentage);
            }
        }
    }
}
