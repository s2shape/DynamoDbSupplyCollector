using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using DynamoDbSupplyCollector;
using DynamoDbSupplyCollectorLoader.Models;
using S2.BlackSwan.SupplyCollector.Models;
using SupplyCollectorDataLoader;

namespace DynamoDbSupplyCollectorLoader
{
    public class DynamoDbSupplyCollectorLoader : SupplyCollectorDataLoaderBase
    {
        public override void InitializeDatabase(DataContainer dataContainer) {
            
        }

        public override void LoadSamples(DataEntity[] dataEntities, long count) {
            using (var client = new DynamoDBClientBuilder(dataEntities[0].Container.ConnectionString).GetClient()) {
                var attrs = new List<AttributeDefinition>();// dataEntities.Select(x => new AttributeDefinition(x.Name, ScalarAttributeType.S)).ToList();
                attrs.Add(new AttributeDefinition("ID", ScalarAttributeType.S));

                client.CreateTableAsync(dataEntities[0].Collection.Name,
                    new List<KeySchemaElement>() {
                        new KeySchemaElement("ID", KeyType.HASH)
                    },
                    attrs, new ProvisionedThroughput(10, 10)
                ).Wait();

                var table = Table.LoadTable(client, dataEntities[0].Collection.Name);

                long rows = 0;
                long batchSize = 100;
                var r = new Random();
                while (rows < count) {
                    if (rows % 1000 == 0) {
                        Console.Write(".");
                    }
                    var batchWrite = table.CreateBatchWrite();
                    for (int i = 0; i < batchSize; i++) {
                        var doc = new Document();
                        doc["ID"] = Guid.NewGuid().ToString();

                        foreach (var dataEntity in dataEntities) {
                            switch (dataEntity.DataType) {
                                case DataType.String:
                                    doc[dataEntity.Name] = new Guid().ToString();
                                    break;
                                case DataType.Int:
                                    doc[dataEntity.Name] = r.Next().ToString();
                                    break;
                                case DataType.Double:
                                    doc[dataEntity.Name] = r.NextDouble().ToString();
                                    break;
                                case DataType.Boolean:
                                    doc[dataEntity.Name] = (r.Next(100) > 50).ToString();
                                    break;
                                case DataType.DateTime:
                                    doc[dataEntity.Name] = DateTimeOffset
                                        .FromUnixTimeMilliseconds(
                                            DateTimeOffset.Now.ToUnixTimeMilliseconds() + r.Next()).DateTime.ToString("s");
                                    break;
                                default:
                                    doc[dataEntity.Name] = r.Next().ToString();
                                    break;
                            }
                        }

                        batchWrite.AddDocumentToPut(doc);
                    }

                    batchWrite.ExecuteAsync().Wait();
                    rows += batchSize;
                }
                Console.WriteLine();
            }
        }

        private List<Person> GetPeople(int number)
        {
            var list = new List<Person>(number);

            var noLastName = new Person() { Id = Guid.NewGuid(), FirstName = "Eugene" };
            var deleted = new Person() { Id = Guid.NewGuid(), FirstName = "Eugene (deleted)", IsDeleted = true };
            var noType1 = new Person(1000000, 10, 10);
            noType1.Addresses["type1"] = null;

            list.Add(noType1);
            list.Add(noLastName);
            list.Add(deleted);

            for (int i = 0; i < number; i++)
            {
                var person = new Person(i, 2, 3);
                list.Add(person);
            }

            return list;
        }

        public override void LoadUnitTestData(DataContainer dataContainer) {
            Console.WriteLine($"Using connection string {dataContainer.ConnectionString}");
            using (var client = new DynamoDBClientBuilder(dataContainer.ConnectionString).GetClient()) {
                client.CreateTableAsync("CONTACTS_AUDIT",
                    new List<KeySchemaElement>() {
                        new KeySchemaElement("ID", KeyType.HASH)
                    },
                    new List<AttributeDefinition>() {
                        new AttributeDefinition("ID", ScalarAttributeType.S)
                    }, new ProvisionedThroughput(10, 10)
                ).Wait();
                client.CreateTableAsync("EMAILS",
                    new List<KeySchemaElement>() {
                        new KeySchemaElement("ID", KeyType.HASH)
                    },
                    new List<AttributeDefinition>() {
                        new AttributeDefinition("ID", ScalarAttributeType.S)
                    }, new ProvisionedThroughput(10, 10)
                ).Wait();
                client.CreateTableAsync("LEADS",
                    new List<KeySchemaElement>() {
                        new KeySchemaElement("ID", KeyType.HASH)
                    },
                    new List<AttributeDefinition>() {
                        new AttributeDefinition("ID", ScalarAttributeType.S)
                    }, new ProvisionedThroughput(10, 10)
                ).Wait();
                client.CreateTableAsync("PEOPLE",
                    new List<KeySchemaElement>() {
                        new KeySchemaElement("Id", KeyType.HASH)
                    },
                    new List<AttributeDefinition>() {
                        new AttributeDefinition("Id", ScalarAttributeType.S)
                    }, new ProvisionedThroughput(10, 10)
                ).Wait();


                var contextConfig = new DynamoDBContextConfig() { TableNamePrefix = "" };
                var context = new DynamoDBContext(client, contextConfig);

                var people = GetPeople(200);

                var contactsBatch = context.CreateBatchWrite<Person>();
                contactsBatch.AddPutItems(people);
                contactsBatch.ExecuteAsync().GetAwaiter().GetResult();
            }
        }
    }
}
