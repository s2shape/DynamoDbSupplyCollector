﻿using Amazon.DynamoDBv2.Model;
using System.Collections.Generic;
using Xunit;
using S2.BlackSwan.SupplyCollector.Models;
using FluentAssertions;
using System.IO;
using System.Linq;
using System;
using System.Text;

namespace DynamoDbSupplyCollector.Tests
{
    public class DynamoAttributeExtensionsTests
    {
        private static string Base64String = Convert.ToBase64String(Encoding.ASCII.GetBytes("any string"));

        private Dictionary<string, AttributeValue> _mapSut = new Dictionary<string, AttributeValue>
        {
            {
                "Addresses", new AttributeValue
                {
                    IsMSet = true,
                    M = new Dictionary<string, AttributeValue>
                    {
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
                    }
                }
            }
        };

        private Dictionary<string, AttributeValue> _listSut = new Dictionary<string, AttributeValue>
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

        private Dictionary<string, AttributeValue> _simpleListsSut = new Dictionary<string, AttributeValue>
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

        private Dictionary<string, AttributeValue> _binarySut = new Dictionary<string, AttributeValue>
        {
            {
                "BinaryDataObjects", new AttributeValue
                {
                    BS = new List<MemoryStream>
                    {
                        new MemoryStream(Convert.FromBase64String(Base64String)),
                        new MemoryStream(Convert.FromBase64String(Base64String))
                    },
                }
            },
            {
                "BinaryDataObject", new AttributeValue
                {
                    B = new MemoryStream(Convert.FromBase64String(Base64String)),
                }
            }
        };


        [Fact]
        public void CollectSample_binary_data()
        {
            // act
            var binaryObj = _binarySut.CollectSample("BinaryDataObject");
            var binaryObjs = _binarySut.CollectSample("BinaryDataObjects");

            // assert
            var binaryObjExpected = new List<string> { Base64String };
            binaryObj.Should().BeEquivalentTo(binaryObjExpected);

            var bibaryObjsExpected = new List<string> { Base64String, Base64String };
            binaryObjs.Should().BeEquivalentTo(bibaryObjsExpected);
        }

        [Fact]
        public void CollectSample_simple_lists()
        {
            // act
            var strings = _simpleListsSut.CollectSample("SimpleStrings");
            var ints = _simpleListsSut.CollectSample("SimpleInts");
            var bools = _simpleListsSut.CollectSample("BooleanValue");

            // assert
            var expectedStrings = new List<string> { "val1", "val2" };
            var expectedInts = new List<string> { "1", "-2" };
            var expectedBools = new List<string> { "1" };

            strings.Should().BeEquivalentTo(expectedStrings);
            ints.Should().BeEquivalentTo(expectedInts);
            bools.Should().BeEquivalentTo(expectedBools);
        }

        [Fact]
        public void CollectSample_nested_list_of_objects()
        {
            // act
            var sample = _listSut.CollectSample("PhoneNumbers.CountryCode");

            // assert
            var expected = new List<string> { "CountryCode1", "CountryCode0" };

            sample.Should().BeEquivalentTo(expected);
        }

        [Fact]
        public void CollectSample_nested_object()
        {
            // act
            var sample = _mapSut.CollectSample("Addresses.type0.Street1");

            // assert
            var expected = "Street10";

            sample.Should().HaveCount(1);
            sample.First().Should().Be(expected);
        }

        [Fact]
        public void CollectSample_nested_object_that_does_not_exist()
        {
            // act
            var sample = _mapSut.CollectSample("Addresses.type1.Street1");

            // assert
            sample.Should().HaveCount(0);
        }

        [Fact]
        public void GetSchema_handles_maps()
        {
            // arrange
            var container = new DataContainer();
            var collection = new DataCollection(container, "Any");

            // act
            var schema = _mapSut.GetSchema(container, collection);

            // assert
            var expected = new List<DataEntity>()
            {
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

            // act
            var schema = _listSut.GetSchema(container, collection);

            // assert
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

            // act
            var schema = _simpleListsSut.GetSchema(container, collection);

            // assert
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

            var schema = _binarySut.GetSchema(container, collection);

            var expected = new List<DataEntity>()
            {
                new DataEntity("BinaryDataObjects", DataType.Unknown, "BS", container, collection),
                new DataEntity("BinaryDataObject", DataType.ByteArray, "B", container, collection)
            };

            schema.Should().BeEquivalentTo(expected);
        }
    }
}
