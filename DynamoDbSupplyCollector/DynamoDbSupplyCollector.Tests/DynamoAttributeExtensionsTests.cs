using Amazon.DynamoDBv2.Model;
using System.Collections.Generic;
using Xunit;
using S2.BlackSwan.SupplyCollector.Models;
using FluentAssertions;

namespace DynamoDbSupplyCollector.Tests
{
    public class DynamoAttributeExtensionsTests
    {
        [Fact]
        public void GetSchema_handles_dynamo_maps()
        {
            // arrange
            var container = new DataContainer();
            var collection = new DataCollection(container, "Any");

            var sut = new Dictionary<string, AttributeValue>
            {
                {
                    "Addresses", new AttributeValue
                    {
                        IsMSet = true,
                        M = new Dictionary<string, AttributeValue>
                        {
                            {
                                "type1", new AttributeValue
                                {
                                   IsMSet = true,
                                   M = new Dictionary<string, AttributeValue>
                                   {
                                        { "Zip", new AttributeValue { S = "Zip1" } },
                                        { "Street2", new AttributeValue { S = "Street21" } },
                                        { "Street1", new AttributeValue { S = "Street11" } },
                                        { "City", new AttributeValue { S = "City1" } },
                                        { "State", new AttributeValue { S = "State1" } },
                                   }
                                }
                            },

                             {
                                "type0", new AttributeValue
                                {
                                   IsMSet = true,
                                   M = new Dictionary<string, AttributeValue>
                                   {
                                        { "Zip", new AttributeValue { S = "Zip0" } },
                                        { "Street2", new AttributeValue { S = "Street20" } },
                                        { "Street1", new AttributeValue { S = "Street10" } },
                                        { "City", new AttributeValue { S = "City0" } },
                                        { "State", new AttributeValue { S = "State0" } },
                                   }
                                }
                            }
                        },
                    }
                }
            };

            var schema = sut.GetSchema(container, collection);

            var expected = new List<DataEntity>()
            {
                new DataEntity("Addresses.type1.Zip", DataType.String, "Zip1", container, collection),
                new DataEntity("Addresses.type1.Street2", DataType.String, "Street21", container, collection),
                new DataEntity("Addresses.type1.Street1", DataType.String, "Street11", container, collection),
                new DataEntity("Addresses.type1.City", DataType.String, "City1", container, collection),
                new DataEntity("Addresses.type1.State", DataType.String, "State1", container, collection),

                new DataEntity("Addresses.type0.Zip", DataType.String, "Zip0", container, collection),
                new DataEntity("Addresses.type0.Street2", DataType.String, "Street20", container, collection),
                new DataEntity("Addresses.type0.Street1", DataType.String, "Street10", container, collection),
                new DataEntity("Addresses.type0.City", DataType.String, "City0", container, collection),
                new DataEntity("Addresses.type0.State", DataType.String, "State0", container, collection)
            };

            schema.Should().BeEquivalentTo(expected);
        }

        [Fact]
        public void GetSchema_handles_dynamo_lists()
        {
            // arrange
            var container = new DataContainer();
            var collection = new DataCollection(container, "Any");

            var sut = new Dictionary<string, AttributeValue>
            {
                {
                    "PhoneNumbers", new AttributeValue
                    {
                        IsLSet = true,
                        L = new List<AttributeValue>
                        {
                            new AttributeValue
                            {
                                IsMSet = true,
                                M = new Dictionary<string, AttributeValue>
                                {
                                      { "CountryCode", new AttributeValue { S = "CountryCode0" } },
                                      { "Number", new AttributeValue { S = "Number0" } },
                                      { "Type", new AttributeValue { S = "Type0" } },
                                }
                            },

                            new AttributeValue
                            {
                                IsMSet = true,
                                M = new Dictionary<string, AttributeValue>
                                {
                                      { "CountryCode", new AttributeValue { S = "CountryCode1" } },
                                      { "Number", new AttributeValue { S = "Number1" } },
                                      { "Type", new AttributeValue { S = "Type1" } },
                                }
                            }
                        }
                    }
                }
            };

            var schema = sut.GetSchema(container, collection);

            var expected = new List<DataEntity>()
            {
                new DataEntity("PhoneNumbers.CountryCode", DataType.String, "CountryCode0", container, collection),
                new DataEntity("PhoneNumbers.Number", DataType.String, "Number0", container, collection),
                new DataEntity("PhoneNumbers.Type", DataType.String, "Type0", container, collection)              
            };

            schema.Should().BeEquivalentTo(expected);
        }
    }
}
