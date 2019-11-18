# DynamoDbSupplyCollector
A supply collector designed to connect to DynamoDB

## Build
Run `dotnet build`

## Tests
Run `./run-tests.sh`

## Known issues
- Cannot load 100k samples even within 30 minutes
- Unstable tests.

## Additional info

To run DynamoDB in docker please go to the root directory where docker-complose.yaml is and run this command:
`docker-compose up`

The command runs a DynamoDB instance and creates some test tables.
Two docker containers are supposed to start. The first one is the actual DynamoDB server 
and the second one creates the test tables if they don't exist and stops.
The GUI for the database can be accessed using this link http://localhost:8000/.

To stop the server and clean up the data please execute this command:

`docker-compose down -volume`