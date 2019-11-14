using FluentAssertions;
using System.Linq;
using Xunit;

namespace DynamoDbSupplyCollector.Tests
{
    public class RandomSamplerTests
    {
        [Fact]
        public void Random_resurns_as_many_items_as_SampleSize()
        {
            // arrange
            const int SampleSize = 10;
            var collectionOf100 = Enumerable.Range(0, 100).ToList();
            var sut = new RandomSampler(SampleSize, collectionOf100.Count);

            // act
            var result = sut.Random(collectionOf100);

            result.Should().HaveCount(SampleSize);
            result.Should().OnlyHaveUniqueItems();
        }

        [Fact]
        public void Random_returns_input_when_SampleSize_is_greater_or_equeal_ItemsCount()
        {
            // arrange
            var collectionOf10 = Enumerable.Range(0, 10).ToList();
            const int SampleSize = 100;
            var sut = new RandomSampler(SampleSize, collectionOf10.Count);

            // act
            var result = sut.Random(collectionOf10);

            // assert
            result.Should().BeEquivalentTo(collectionOf10);
        }


        [Fact]
        public void Random_should_reduce_dataset_to_get_samples()
        {
            // arrange
            var collectionOf10_000 = Enumerable.Range(0, 10_000).ToList();
            const int SampleSize = 2;
            var sut = new RandomSampler(SampleSize, collectionOf10_000.Count);

            // act
            var result = sut.Random(collectionOf10_000.Take(1000).ToList());

            // assert
            var dataShouldUsedForSamples = collectionOf10_000.Take(20).ToList();

            result.Should().OnlyContain(x => dataShouldUsedForSamples.Contains(x));
            result.Should().HaveCount(SampleSize);
        }
    }
}
