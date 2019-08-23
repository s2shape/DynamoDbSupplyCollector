# DynamoDbSupplyCollector
A supply collector designed to connect to DynamoDB

To run DynamoDB in docker please go to the root directory where docker-complose.yaml is and run this command:

`docker-compose up`

The command runs a DynamoDB instance and creates some test tables.
Two docker containers are supposed to start. The first one is the actual DynamoDB server 
and the second one creates the test tables if they don't exist and stops.
The GUI for the database can be accessed using this link http://localhost:8000/.

Before you run the tests please go to `\DynamoDbSupplyCollector\DynamoDbDataLoader` and execute 
`dotnet run` command to seed the database with test data.

The command to run the tests is `dotnet test` but before you should go to this folder `\DynamoDbSupplyCollector\DynamoDbSupplyCollector.Tests`. 

To stop the server and clean up the data please execute this command:

`docker-compose down -volume`