using Amazon.DynamoDBv2.DocumentModel;
using FluentAssertions;
using Newtonsoft.Json;
using S2.BlackSwan.SupplyCollector.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace DynamoDbSupplyCollector.Tests
{
    // Try to access a nested object.
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
        public void CollectSample_handles_nulls_properly()
        {
            // arrange
            var dataCollection = new DataCollection(_container, "PEOPLE");
            var dataEntity = new DataEntity("AlwaysNull", DataType.Unknown, "NULL", _container, dataCollection);
            const int SampleSize = 202;

            var sample = _sut.CollectSample(dataEntity, SampleSize);

            // assert
            sample.Should().HaveCount(SampleSize);
            sample.Should().OnlyContain(s => string.IsNullOrEmpty(s));
        }

        [Fact]
        public void CollectSample_skips_complex_objects()
        {
            // arrange
            var dataCollection = new DataCollection(_container, "PEOPLE");
            var dataEntity = new DataEntity("PhoneNumbers", DataType.Unknown, "List", _container, dataCollection);
            const int SampleSize = 202;

            var sample = _sut.CollectSample(dataEntity, SampleSize);

            // assert
            sample.Should().HaveCount(0);
        }

        [Fact]
        public void GetSchema()
        {
            var schema = _sut.GetSchema(_container);
            //Document.FromAttributeMap


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
