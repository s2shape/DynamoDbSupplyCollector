# DynamoDbSupplyCollector
A supply collector designed to connect to DynamoDB

To run DynamoDB in docker please go to the root directory where docker-complose.yaml is and run this command:

`docker-compose up`

The command runs a DynamoDB instance and creates some test tables.
The GUI for the database can be accessed using this link http://localhost:8000/.

To stop the server and clean up the data please execute this command:

`docker-compose down -volume`