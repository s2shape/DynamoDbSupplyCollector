using FluentAssertions;
using S2.BlackSwan.SupplyCollector.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace DynamoDbSupplyCollector.Tests
{
    public class DynamoDbSupplyCollectorTests : IClassFixture<LaunchSettingsFixture>
    {
        private LaunchSettingsFixture _fixture;
        private DynamoDbSupplyCollector _sut;
        private DataContainer _container;

        readonly List<string> KNOWN_TABLES = new List<string> { "CONTACTS_AUDIT", "EMAILS", "LEADS", "PEOPLE" };
        
        public DynamoDbSupplyCollectorTests(LaunchSettingsFixture fixture)
        {
            _fixture = fixture;
            _sut = new DynamoDbSupplyCollector();

            var host = Environment.GetEnvironmentVariable("DYNAMODB_HOST");
            var port = Environment.GetEnvironmentVariable("DYNAMODB_PORT");
            var keyId = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID");
            var secretKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY");

            _container = new DataContainer
            {
                ConnectionString = $"ServiceURL=http://{host}:{port}; AccessKey={keyId}; AccessSecret={secretKey}"
            };
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
        public void CollectSample_does_not_throw()
        {
            // get the schema and make sure that CollectSample doesn't throw any exception for every data entity
            var (dataCollections, dataEntities) = _sut.GetSchema(_container);

            Action act = () => dataEntities
                //.Where(d => d.Name == "Addresses.type1.Street1")
                .Select(d => _sut.CollectSample(d, 500)).ToList();

            act.Should().NotThrow();
        }

        [Fact]
        public void CollectSample_handles_strings_properly()
        {
            // arrange
            var dataCollection = new DataCollection(_container, "PEOPLE");
            var dataEntity = new DataEntity("FirstName", DataType.String, "string", _container, dataCollection);
            const int SampleSize = 10;

            var sample = _sut.CollectSample(dataEntity, SampleSize);

            // assert
            sample.Should().HaveCount(SampleSize);
            sample.Should().OnlyContain(x => !string.IsNullOrEmpty(x));
        }

        [Fact]
        public void CollectSample_handles_numbers_properly()
        {
            // arrange
            var dataCollection = new DataCollection(_container, "PEOPLE");
            var dataEntity = new DataEntity("Age", DataType.Int, "Integer", _container, dataCollection);
            const int SampleSize = 10;

            var sample = _sut.CollectSample(dataEntity, SampleSize);

            // assert
            sample.Should().HaveCount(SampleSize);
            sample.Should().OnlyContain(s => IsInt(s));
        }

        [Fact]
        public void CollectSample_handles_dates_properly()
        {
            // arrange
            var dataCollection = new DataCollection(_container, "PEOPLE");
            var dataEntity = new DataEntity("DOB", DataType.DateTime, "Date", _container, dataCollection);
            const int SampleSize = 10;

            var sample = _sut.CollectSample(dataEntity, SampleSize);

            // assert
            sample.Should().HaveCount(SampleSize);
            sample.Should().OnlyContain(s => IsDate(s));
        }

        [Fact]
        public void CollectSample_handles_booleans_properly()
        {
            // arrange
            var dataCollection = new DataCollection(_container, "PEOPLE");
            var dataEntity = new DataEntity("IsDeleted", DataType.Boolean, "Boolean", _container, dataCollection);
            const int SampleSize = 202;

            var sample = _sut.CollectSample(dataEntity, SampleSize);

            // assert
            sample.Should().HaveCount(SampleSize);
            sample.Should().OnlyContain(s => IsBoolean(s));
        }

        [Fact]
        public void GetSchema()
        {
            var (dataCollections, dataEntities) = _sut.GetSchema(_container);

            dataCollections.Should().HaveCount(4);
            dataEntities.Should().NotBeEmpty();
        }

        private bool IsInt(string val)
        {
            return int.TryParse(val, out var _);
        }

        private bool IsDate(string val)
        {
            return DateTime.TryParse(val, out var _);
        }

        /// <summary>
        /// Boolean value stores as a number. It cant either 1 or 0.
        /// </summary>
        private bool IsBoolean(string val)
        {
            return val == "1" || val == "0";
        }
    }
}
