using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using System;

namespace DynamoDbSupplyCollector
{
    internal class DynamoDBClientBuilder
    {
        private string _connectionString;

        public DynamoDBClientBuilder(string connectionString)
        {
            _connectionString = connectionString;
        }

        public AmazonDynamoDBClient GetClient()
        {
            var sections = _connectionString.Split(';');

            if (sections.Length < 3)
            {
                throw new FormatException("Invalid connection string format for DynamoDB. " +
                    "The valid format is " +
                    "'ServiceURL={YOUR_SERVICE_URL}; AccessKey={YOUR_KEY}; AccessSecret={YOUR_SECRET}");
            }

            var serviceUrlSection = sections[0];

            var keyIdSection = sections[1];

            var accessKeySection = sections[2];

            var clientConfig = new AmazonDynamoDBConfig() { ServiceURL = Value(serviceUrlSection) };
            var client = new AmazonDynamoDBClient(Value(keyIdSection), Value(accessKeySection), clientConfig);

            return client;
        }

        public DynamoDBContext GetContext(string connectionString)
        {
            var contextConfig = new DynamoDBContextConfig() { TableNamePrefix = "" };
            var context = new DynamoDBContext(GetClient(), contextConfig);

            return context;
        }

        private string Value(string pair)
        {
            return pair.Split("=")[1];
        }
    }
}
