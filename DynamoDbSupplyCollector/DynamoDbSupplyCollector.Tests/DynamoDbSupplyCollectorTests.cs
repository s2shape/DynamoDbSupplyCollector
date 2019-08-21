using FluentAssertions;
using S2.BlackSwan.SupplyCollector.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace DynamoDbSupplyCollector.Tests
{
    public class DynamoDbSupplyCollectorTests
    {
        private DynamoDbSupplyCollector _sut;

        readonly List<string> KNOWN_TABLES = new List<string> { "CONTACTS_AUDIT", "EMAILS", "LEADS", "PEOPLE" };

        public DynamoDbSupplyCollectorTests()
        {
            _sut = new DynamoDbSupplyCollector();
        }

        [Fact]
        public void DataStoreTypes_returns_dynamodb()
        {
            _sut.DataStoreTypes().Should().Contain("DynamoDB");
        }

        [Fact]
        public void TestConnection()
        {
            // arrange
            var container = new DataContainer
            {
                ConnectionString = "ServiceURL=http://localhost:8000; AccessKey=key_id; AccessSecret=access_key"
            };

            _sut.TestConnection(container).Should().BeTrue();
        }

        [Fact]
        public void GetDataCollectionMetrics()
        {
            // arrange
            var container = new DataContainer
            {
                ConnectionString = "ServiceURL=http://localhost:8000; AccessKey=key_id; AccessSecret=access_key"
            };

            // act
            var metrics = _sut.GetDataCollectionMetrics(container);

            // assert

            metrics.Select(m => m.Name).Should().BeEquivalentTo(KNOWN_TABLES);
            metrics.Should().HaveCount(KNOWN_TABLES.Count);
        }
    }
}
