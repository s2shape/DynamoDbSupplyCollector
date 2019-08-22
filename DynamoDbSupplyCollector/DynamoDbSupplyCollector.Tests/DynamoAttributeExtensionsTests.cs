using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xunit;

namespace DynamoDbSupplyCollector.Tests
{
    public class DynamoAttributeExtensionsTests
    {
        //string testSample = "" +
        //    $"\"Addresses\": { }";

        [Fact]
        public void GetSchema()
        {
            var sample = new Dictionary<string, AttributeValue>
            {
                {
                    "Addresses", new AttributeValue
                    {
                        M = new Dictionary<string, AttributeValue>
                        {
                            {
                                "type1", new AttributeValue
                                {
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

                        //B = null, BOOL = false, IsBOOLSet = false, BS = new List<MemoryStream>(), L = new List<AttributeValue>(), IsLSet = false,
                        
                    }
                }
            };
            // arrange


        }
    }
}
