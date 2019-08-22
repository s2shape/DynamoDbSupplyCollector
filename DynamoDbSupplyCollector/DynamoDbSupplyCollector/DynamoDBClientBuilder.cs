using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;

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
            //TODO: re-factor handling of the connection string
            var sections = _connectionString.Split(';');

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
