using FluentAssertions;
using System;
using Xunit;

namespace DynamoDbSupplyCollector.Tests
{
    public class DynamoDbSupplyCollectorTests
    {
        private DynamoDbSupplyCollector _sut;

        public DynamoDbSupplyCollectorTests()
        {
            _sut = new DynamoDbSupplyCollector();
        }

        [Fact]
        public void DataStoreTypes_returns_dynamodb()
        {
            _sut.DataStoreTypes().Should().Contain("DynamoDB");
        }
    }
}
