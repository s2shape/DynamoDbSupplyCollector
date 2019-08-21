var AWS = require("aws-sdk");
var retry = require('retry');

AWS.config.update({
    region: "us-west-2",
    endpoint: "http://dynamo:8000"
});

var dynamodb = new AWS.DynamoDB();
var tables = [
    {
        TableName: "CONTACTS_AUDIT",
        KeySchema: [
            { AttributeName: "ID", KeyType: "HASH" }
        ],
        AttributeDefinitions: [
            { AttributeName: "ID", AttributeType: "S" }
        ],
        ProvisionedThroughput: {
            ReadCapacityUnits: 10,
            WriteCapacityUnits: 10
        }
    },
	
	{
        TableName: "EMAILS",
        KeySchema: [
            { AttributeName: "ID", KeyType: "HASH" }
        ],
        AttributeDefinitions: [
            { AttributeName: "ID", AttributeType: "S" }
        ],
        ProvisionedThroughput: {
            ReadCapacityUnits: 10,
            WriteCapacityUnits: 10
        }
    },
	
	{
        TableName: "LEADS",
        KeySchema: [
            { AttributeName: "ID", KeyType: "HASH" }
        ],
        AttributeDefinitions: [
            { AttributeName: "ID", AttributeType: "S" }
        ],
        ProvisionedThroughput: {
            ReadCapacityUnits: 10,
            WriteCapacityUnits: 10
        }
    }
];

function createTables(callBack) {
    console.log("Create Tables Started");
    tables.forEach(table => tryCreateTable(table, callBack));
    console.log("Create Tables Finished");
}

function tryCreateTable(table, callBack) {
    var operation = retry.operation({
        retries: 10,
        factor: 2,
        minTimeout: 1 * 1000,
        maxTimeout: 3 * 1000,
        randomize: true,
      });

    operation.attempt(function(currentAttempt) {
        console.log(`Attempt create table ${table.TableName} #${currentAttempt}`);

        dynamodb.createTable(table, (err, data) => {
            if (operation.retry(err)) {
                console.log(JSON.stringify(err))
                return;
              }
              callBack(err ? operation.mainError() : null, data);
        });
    });
}

createTables(function(err, table) {
    if (err) {
        console.error(err);
    } 
    else {
    console.log(`Created table. Table description JSON:${JSON.stringify(table, null, 2)}`);
    }
});