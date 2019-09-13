#!/bin/bash

export DYNAMODB_HOST=localhost
export DYNAMODB_PORT=8000
export AWS_ACCESS_KEY_ID=key_id
export AWS_SECRET_ACCESS_KEY=access_key

docker-compose up -d

sleep 10

cd DynamoDbSupplyCollector
dotnet build

cd DynamoDbDataLoader
dotnet run

cd ..
dotnet test

docker-compose down -v