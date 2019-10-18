using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DynamoDbSupplyCollector
{
    public class DynamoDbSupplyCollector : SupplyCollectorBase
    {
        const int DEFAULT_SCHEMA_SAMPLE_SIZE = 10;

        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize)
        {
            using (var client = new DynamoDBClientBuilder(dataEntity.Container.ConnectionString).GetClient())
            {
                var itemsCount = GetItemsCount(dataEntity.Collection.Name, client);

                if (itemsCount == 0)
                    return Enumerable.Empty<string>().ToList();

                var randomSampler = new RandomSampler(sampleSize, itemsCount, 10);

                var datasource = randomSampler.Random(limit =>
                {
                    var request = new ScanRequest
                    {
                        TableName = dataEntity.Collection.Name,
                        Limit = (int)limit
                    };

                    var response = client.ScanAll(request).GetAwaiter().GetResult();

                    return response;
                });

                var samples = datasource
                    .SelectMany(x => x.CollectSample(dataEntity.Name))
                    .ToList();

                return samples;
            }
        }

        public override List<string> DataStoreTypes()
        {
            return new List<string>() { "DynamoDB" };
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container)
        {
            using (var client = new DynamoDBClientBuilder(container.ConnectionString).GetClient())
            {
                var tables = client.GetTables().GetAwaiter().GetResult();

                var metrics = new List<DataCollectionMetrics>();
                foreach (var table in tables)
                {
                    var request = new DescribeTableRequest(table);

                    var response = client.DescribeTableAsync(request).GetAwaiter().GetResult();

                    var tbl = response.Table;
                    metrics.Add(new DataCollectionMetrics
                    {
                        Name = tbl.TableName,
                        UsedSpaceKB = tbl.TableSizeBytes / 1000,
                        TotalSpaceKB = tbl.TableSizeBytes / 1000,
                        RowCount = tbl.ItemCount,
                    });
                }

                return metrics;
            }
        }

        public override (List<DataCollection>, List<DataEntity>) GetSchema(DataContainer container)
        {
            using (var client = new DynamoDBClientBuilder(container.ConnectionString).GetClient())
            {
                var tableNames = client.GetTables().GetAwaiter().GetResult();

                var dataEntities = tableNames.SelectMany(t => GetSchema(t, client, container));
                var dataCollections = tableNames.Select(t => new DataCollection(container, t));

                return (dataCollections.ToList(), dataEntities.ToList());
            }
        }

        private long GetItemsCount(string tableName, AmazonDynamoDBClient client)
        {
            var request = new DescribeTableRequest(tableName);

            var response = client.DescribeTableAsync(request).GetAwaiter().GetResult();

            return response.Table.ItemCount;
        }

        private List<DataEntity> GetSchema(string tableName, AmazonDynamoDBClient client, DataContainer container)
        {
            var request = new ScanRequest
            {
                TableName = tableName,
                Limit = DEFAULT_SCHEMA_SAMPLE_SIZE
            };

            var samples = client.ScanAsync(request).GetAwaiter().GetResult();

            var dataCollection = new DataCollection(container, tableName);

            var dataEntities = samples.Items
                .SelectMany(s => s.GetSchema(container, dataCollection))
                .DistinctBy(s => s.Name)
                .ToList();

            return dataEntities;
        }

        public override bool TestConnection(DataContainer container)
        {
            using (var client = new DynamoDBClientBuilder(container.ConnectionString).GetClient())
            {
                var request = new ListTablesRequest
                {
                    Limit = 1,
                    ExclusiveStartTableName = null
                };

                try
                {
                    client.ListTablesAsync(request).Wait();
                    return true;
                }
                catch (Exception)
                {
                    return false;
                }
            }
        }
    }
}
