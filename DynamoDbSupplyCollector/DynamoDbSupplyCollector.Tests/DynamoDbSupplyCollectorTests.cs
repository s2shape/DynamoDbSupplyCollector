using FluentAssertions;
using S2.BlackSwan.SupplyCollector.Models;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace DynamoDbSupplyCollector.Tests
{
    public class DynamoDbSupplyCollectorTests
    {
        private DynamoDbSupplyCollector _sut;
        private DataContainer _container = new DataContainer
        {
            ConnectionString = "ServiceURL=http://localhost:8000; AccessKey=key_id; AccessSecret=access_key"
        };
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
            _sut.TestConnection(_container).Should().BeTrue();
        }

        [Fact]
        public void GetDataCollectionMetrics()
        {
            // act
            var metrics = _sut.GetDataCollectionMetrics(_container);

            // assert
            metrics.Select(m => m.Name).Should().BeEquivalentTo(KNOWN_TABLES);
            metrics.Should().HaveCount(KNOWN_TABLES.Count);
        }

        [Fact]
        public void CollectSample()
        {
            // arrange
            var dataCollection = new DataCollection(_container, "PEOPLE");
            var firstNameEntity = new DataEntity("FirstName", DataType.String, "string", _container, dataCollection);
            const int SampleSize = 10;

            var sample = _sut.CollectSample(firstNameEntity, SampleSize);

            // assert
            sample.Should().HaveCount(SampleSize);
            
        }
    }
}
