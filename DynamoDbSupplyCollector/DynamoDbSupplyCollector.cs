using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DynamoDbSupplyCollector
{
    public class DynamoDbSupplyCollector : SupplyCollectorBase
    {
        const int DEFAULT_SCHEMA_SAMPLE_SIZE = 10;

        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize)
        {
            return AsyncHelpers.RunSync(() => CollectSampleAsync(dataEntity, sampleSize));
        }

        public async Task<List<string>> CollectSampleAsync(DataEntity dataEntity, int sampleSize)
        {
            using (var client = new DynamoDBClientBuilder(dataEntity.Container.ConnectionString).GetClient())
            {
                var itemsCount = await GetItemsCount(dataEntity.Collection.Name, client);

                if (itemsCount == 0)
                    return Enumerable.Empty<string>().ToList();

                var randomSampler = new RandomSampler(sampleSize, itemsCount, 10);

                var request = new ScanRequest
                {
                    TableName = dataEntity.Collection.Name,
                    Limit = (int)sampleSize
                };

                var response = await client.ScanAll(request).ConfigureAwait(false); //randomSampler.Random(

                return response.SelectMany(x => x.CollectSample(dataEntity.Name)).Take(sampleSize).ToList();
            }
        }

        public override List<string> DataStoreTypes()
        {
            return new List<string>() { "DynamoDB" };
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container)
        {
            return AsyncHelpers.RunSync(() => GetDataCollectionMetricsAsync(container));
        }

        public async Task<List<DataCollectionMetrics>> GetDataCollectionMetricsAsync(DataContainer container)
        {
            using (var client = new DynamoDBClientBuilder(container.ConnectionString).GetClient()) {
                var tables = await client.GetTables().ConfigureAwait(false);

                var metrics = new List<DataCollectionMetrics>();
                foreach (var table in tables)
                {
                    var request = new DescribeTableRequest(table);

                    var response = await client.DescribeTableAsync(request).ConfigureAwait(false);

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
            return AsyncHelpers.RunSync(() => GetSchemaAsync(container));
        }

        public async Task<(List<DataCollection>, List<DataEntity>)> GetSchemaAsync(DataContainer container)
        {
            using (var client = new DynamoDBClientBuilder(container.ConnectionString).GetClient())
            {
                var tableNames = await client.GetTables().ConfigureAwait(false);

                var dataEntities = new List<DataEntity>();
                foreach (var tableName in tableNames) {
                    dataEntities.AddRange(await GetSchema(tableName, client, container));
                }
                
                var dataCollections = tableNames.Select(t => new DataCollection(container, t));

                return (dataCollections.ToList(), dataEntities.ToList());
            }
        }

        private async Task<long> GetItemsCount(string tableName, AmazonDynamoDBClient client)
        {
            var request = new DescribeTableRequest(tableName);

            var response = await client.DescribeTableAsync(request).ConfigureAwait(false);

            return response.Table.ItemCount;
        }

        private async Task<List<DataEntity>> GetSchema(string tableName, AmazonDynamoDBClient client, DataContainer container)
        {
            var request = new ScanRequest
            {
                TableName = tableName,
                Limit = DEFAULT_SCHEMA_SAMPLE_SIZE
            };

            var samples = await client.ScanAsync(request).ConfigureAwait(false);

            var dataCollection = new DataCollection(container, tableName);

            var dataEntities = samples.Items
                .SelectMany(s => s.GetSchema(container, dataCollection))
                .DistinctBy(s => s.Name)
                .ToList();

            return dataEntities;
        }

        public override bool TestConnection(DataContainer container)
        {
            return AsyncHelpers.RunSync(() => TestConnectionAsync(container));
        }

        public async Task<bool> TestConnectionAsync(DataContainer container)
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
                    await client.ListTablesAsync(request).ConfigureAwait(false);
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
