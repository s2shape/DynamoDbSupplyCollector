using Amazon.DynamoDBv2.Model;
using System.Collections.Generic;
using Xunit;
using S2.BlackSwan.SupplyCollector.Models;
using FluentAssertions;
using System.IO;

namespace DynamoDbSupplyCollector.Tests
{
    public class DynamoAttributeExtensionsTests
    {
        [Fact]
        public void GetSchema_handles_maps()
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
                new DataEntity("Addresses.type1.Zip", DataType.String, "S", container, collection),
                new DataEntity("Addresses.type1.Street2", DataType.String, "S", container, collection),
                new DataEntity("Addresses.type1.Street1", DataType.String, "S", container, collection),
                new DataEntity("Addresses.type1.City", DataType.String, "S", container, collection),
                new DataEntity("Addresses.type1.State", DataType.String, "S", container, collection),

                new DataEntity("Addresses.type0.Zip", DataType.String, "S", container, collection),
                new DataEntity("Addresses.type0.Street2", DataType.String, "S", container, collection),
                new DataEntity("Addresses.type0.Street1", DataType.String, "S", container, collection),
                new DataEntity("Addresses.type0.City", DataType.String, "S", container, collection),
                new DataEntity("Addresses.type0.State", DataType.String, "S", container, collection)
            };

            schema.Should().BeEquivalentTo(expected);
        }

        [Fact]
        public void GetSchema_handles_lists_of_objects()
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
                new DataEntity("PhoneNumbers.CountryCode", DataType.String, "S", container, collection),
                new DataEntity("PhoneNumbers.Number", DataType.String, "S", container, collection),
                new DataEntity("PhoneNumbers.Type", DataType.String, "S", container, collection)
            };

            schema.Should().BeEquivalentTo(expected);
        }

        [Fact]
        public void GetSchema_handles_simple_lists()
        {
            // arrange
            var container = new DataContainer();
            var collection = new DataCollection(container, "Any");

            var sut = new Dictionary<string, AttributeValue>
            {
                {
                    "SimpleStrings", new AttributeValue
                    {
                        SS = new List<string>{ "val1", "val2" },
                    }
                },
                {
                    "SimpleInts", new AttributeValue
                    {
                        NS = new List<string>{ "1", "-2" },
                    }
                },
                {
                    "BooleanValue", new AttributeValue
                    {
                        IsBOOLSet = true,
                        BOOL = true,
                    }
                }
            };

            var schema = sut.GetSchema(container, collection);

            var expected = new List<DataEntity>()
            {
                new DataEntity("SimpleStrings", DataType.Unknown, "SS", container, collection),
                new DataEntity("SimpleInts", DataType.Unknown, "NS", container, collection),
                new DataEntity("BooleanValue", DataType.Boolean, "BOOL", container, collection)
            };

            schema.Should().BeEquivalentTo(expected);
        }

        [Fact]
        public void GetSchema_handles_binary_data()
        {
            // arrange
            var container = new DataContainer();
            var collection = new DataCollection(container, "Any");

            var sut = new Dictionary<string, AttributeValue>
            {
                {
                    "BinaryDataObjects", new AttributeValue
                    {
                        BS = new List<MemoryStream> { new MemoryStream() },
                    }
                },
                {
                    "BinaryDataObject", new AttributeValue
                    {
                        B = new MemoryStream(),
                    }
                }
            };

            var schema = sut.GetSchema(container, collection);

            var expected = new List<DataEntity>()
            {
                new DataEntity("BinaryDataObjects", DataType.Unknown, "BS", container, collection),
                new DataEntity("BinaryDataObject", DataType.ByteArray, "B", container, collection)
            };

            schema.Should().BeEquivalentTo(expected);
        }
    }
}
