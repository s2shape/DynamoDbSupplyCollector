using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using DynamoDbDataLoader.Models;
using System;

namespace DynamoDbDataLoader
{
    class Program
    {
        static void Main(string[] args)
        {
            try {
                var host = Environment.GetEnvironmentVariable("DYNAMODB_HOST");
                if (String.IsNullOrEmpty(host))
                    host = "localhost";
                var port = Environment.GetEnvironmentVariable("DYNAMODB_PORT");
                if (String.IsNullOrEmpty(port))
                    port = "8000";
                var keyId = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID");
                if (String.IsNullOrEmpty(keyId))
                    keyId = "key_id";
                var secretKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY");
                if (String.IsNullOrEmpty(secretKey))
                    secretKey = "access_key";

                var clientConfig = new AmazonDynamoDBConfig() { ServiceURL = $"http://{host}:{port}" };
                var client = new AmazonDynamoDBClient(keyId, secretKey, clientConfig);

                var contextConfig = new DynamoDBContextConfig() { TableNamePrefix = "" };
                var context = new DynamoDBContext(client, contextConfig);

                var dataProvider = new SampleDataProvider();
                var people = dataProvider.GetPeople(200);

                // bulk insert
                var contactsBatch = context.CreateBatchWrite<Person>();
                contactsBatch.AddPutItems(people);
                contactsBatch.ExecuteAsync().GetAwaiter().GetResult();

                Console.WriteLine("The data has been loaded!");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }        
        }
    }
}
