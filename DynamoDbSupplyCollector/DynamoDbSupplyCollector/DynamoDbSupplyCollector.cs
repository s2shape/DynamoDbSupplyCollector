﻿using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DynamoDbSupplyCollector
{
    public class DynamoDbSupplyCollector : SupplyCollectorBase
    {
        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize)
        {
            throw new NotImplementedException();
        }

        public override List<string> DataStoreTypes()
        {
            return new List<string>() { "DynamoDB" };
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container)
        {
            var client = GetClient(container.ConnectionString);

            var tables = GetTables(client).GetAwaiter().GetResult();

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

        public override (List<DataCollection>, List<DataEntity>) GetSchema(DataContainer container)
        {
            throw new NotImplementedException();
        }

        public override bool TestConnection(DataContainer container)
        {
            var client = GetClient(container.ConnectionString);

            var request = new ListTablesRequest
            {
                Limit = 100,
                ExclusiveStartTableName = null
            };

            try
            {
                client.ListTablesAsync(request).GetAwaiter().GetResult();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private AmazonDynamoDBClient GetClient(string connectionString)
        {
            var sections = connectionString.Split(';');

            var serviceUrlSection = sections[0];

            var keyIdSection = sections[1];

            var accessKeySection = sections[2];

            var clientConfig = new AmazonDynamoDBConfig() { ServiceURL = Value(serviceUrlSection) };
            var client = new AmazonDynamoDBClient(Value(keyIdSection), Value(accessKeySection), clientConfig);

            return client;
        }

        private DynamoDBContext GetContext(string connectionString)
        {
            var contextConfig = new DynamoDBContextConfig() { TableNamePrefix = "" };
            var context = new DynamoDBContext(GetClient(connectionString), contextConfig);

            return context;
        }

        string Value(string pair)
        {
            return pair.Split("=")[1];
        }

        async Task<List<string>> GetTables(AmazonDynamoDBClient client)
        {
            var tableNames = new List<string>();
            string lastTableNameEvaluated = null;

            do
            {
                // needs to be in a loop because ListTablesAsync can return up to 100 records at a time
                var request = new ListTablesRequest
                {
                    Limit = 100,
                    ExclusiveStartTableName = lastTableNameEvaluated
                };

                var response = await client.ListTablesAsync(request);
                foreach (string name in response.TableNames)
                    tableNames.Add(name);

                lastTableNameEvaluated = response.LastEvaluatedTableName;
            } while (lastTableNameEvaluated != null);

            return tableNames;
        }
    }
}