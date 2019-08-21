using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using DynamoDbDataLoader.Models;
using System;
using System.Threading.Tasks;

namespace DynamoDbDataLoader
{
    class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                var clientConfig = new AmazonDynamoDBConfig() { ServiceURL = "http://localhost:8000" };
                var client = new AmazonDynamoDBClient("key_id", "access_key", clientConfig);

                var contextConfig = new DynamoDBContextConfig() { TableNamePrefix = "" };
                var context = new DynamoDBContext(client, contextConfig);

                var dataProvider = new SampleDataProvider();


                var people = dataProvider.GetPeople(200);

                // bulk insert
                var contactsBatch = context.CreateBatchWrite<Person>();
                contactsBatch.AddPutItems(people);
                await contactsBatch.ExecuteAsync();

                Console.WriteLine("The data has been loaded!");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }        
        }
    }
}
