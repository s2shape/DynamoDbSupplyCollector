#!/bin/bash

export DYNAMODB_HOST=localhost
export DYNAMODB_PORT=8000
export AWS_ACCESS_KEY_ID=key_id
export AWS_SECRET_ACCESS_KEY=access_key

docker-compose up -d

sleep 10

dotnet restore -s https://www.myget.org/F/s2/ -s https://api.nuget.org/v3/index.json
dotnet build
dotnet publish

pushd DynamoDbSupplyCollectorLoader/bin/Debug/netcoreapp2.2/publish
dotnet SupplyCollectorDataLoader.dll -xunit DynamoDbSupplyCollector http://localhost:8000;AccessKey=key_id;AccessSecret=access_key
popd

dotnet test


docker-compose down -v