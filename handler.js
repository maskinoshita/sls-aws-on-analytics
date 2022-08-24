'use strict';

const AWS = require('aws-sdk');
const Athena = new AWS.Athena({});
const retry = require('async-retry');

const WEBSOCKET_ENDPOINT = process.env.WEBSOCKET_ENDPOINT;
const MANAGEMENT_ENDPOINT = WEBSOCKET_ENDPOINT.replace('wss', 'https');
const CONNECTION_TABLE_NAME = process.env.CONNECTION_TABLE_NAME;
const DATABASE_NAME = process.env.DATABASE_NAME;
const WORKGROUP_NAME = process.env.WORKGROUP_NAME;

class ConnectionGoneError extends Error {
  constructor(err, connectionId) {
    super(err.message);
    this.name = "ConnectionGoneError";
    this.statusCode = err.statusCode;
    this.connectionId = connectionId;
  }
}

const sendMessageToClient = (url, connectionId, payload) => {
  return new Promise((resolve, reject) => {
    const apigatewayManagementApi = new AWS.ApiGatewayManagementApi({
      apiVersion: '2018-11-29',
      endpoint: url
    });
    apigatewayManagementApi.postToConnection({
      ConnectionId: connectionId,
      Data: JSON.stringify(payload)
    }, (err, data) => {
      if (err) {
        reject((err.statusCode === 410)? new ConnectionGoneError(err, connectionId) : err);
      }
      resolve(data);
    });
  });
};

const getProcessedData = async () => {
  // start athena query
  let exec = Athena.startQueryExecution({
    QueryString: `
      SELECT activity_type, device_id, device_temp, track_id, uuid, track_name, artist_name
      FROM processed-data2
    `,
    WorkGroup: WORKGROUP_NAME
  }).promise();

  // wait query done (polling the query execution state using retry)
  exec = exec.then(async (data) => {
    const params = {
      QueryExecutionId: data.QueryExecutionId
    };
    return retry(
      Athena.getQueryExecution(params).promise()
      .then((data) => {
        if(data && data.QueryExecution && data.QueryExecution.Status && data.QueryExecution.Status === "SUCCEEDED") {
          return params;
        } else {
          throw new Error("Query is not done. Retry.");
        }
      }),
      {
        retries: 10,
        minTimeout: 1000
      }
    );
  });

  // get all by iterating 
  const params = { MaxResults: 1000, ...(await exec) }
  let result = await Athena.getQueryResults(params).promise();

  if(!(result.ResultSet && result.ResultSet.Rows)) return [];
  // format result
  const format = (row) => [
    row[0].VarCharValue, // activity_type
    row[1].VarCharValue, // device_id
    row[2].VarCharValue, // device_temp
    row[3].VarCharValue, // track_id
    row[4].VarCharValue, // uuid
    row[5].VarCharValue, // track_name
    row[6].VarCharValue, // artist_name
  ];

  let values = result.ResultSet.Rows.map(row => format(row.Data));
  let nextToken = result.NextToken; // if not null, it has next page
  while(nextToken) {
    result = await Athena.getQueryResults({ NextToken: nextToken , ...params }).promise();
    if(!(result.ResultSet && result.ResultSet.Rows)) break;
    values = values.concat(result.ResultSet.Rows.map(row => format(row.Data)));
    nextToken = result.NextToken;
  }
  return values;
}

const handleUpdate = async (event) => {
  // try {
  //   const payload = await analyze(event.Records);
  //   // push to client
  //   let connectionData = await dynamodb.scan({ TableName: CONNECTION_TABLE_NAME, ProjectionExpression: 'connectionId' }).promise();
  //   let promises = [];
  //   let deletePromises = [];
  //   connectionData.Items.map(async (item) => {
  //     promises.push(sendMessageToClient(MANAGEMENT_ENDPOINT, item.connectionId.S, payload));
  //   });
  //   try {
  //     await Promise.all(promises);
  //   } catch (err) {
  //     if (err instanceof ConnectionGoneError) {
  //       const deleteParams = {
  //         TableName: CONNECTION_TABLE_NAME,
  //         Key: {
  //           connectionId: { S: err.connectionId }
  //         }
  //       };
  //       deletePromises.push(dynamodb.deleteItem(deleteParams).promise());
  //     }
  //   }
  //   // wait remove invalid connections
  //   if (deletePromises.length != 0) {
  //     try {
  //       await Promise.all(deletePromises);
  //     } catch (err) {
  //       console.log(err);
  //     }
  //   }
  //   return {
  //       statusCode: 200
  //   };
  // } catch (err) {
  //   console.log("Error:", err);
  //   return {
  //       statusCode: 500
  //   };
  // }
  console.log(event);
  console.log({ WEBSOCKET_ENDPOINT, MANAGEMENT_ENDPOINT, CONNECTION_TABLE_NAME, DATABASE_NAME, WORKGROUP_NAME });
}

const connect = async (event) => {
  try {
    const connectionId = event.requestContext.connectionId;
    const putParams = {
      TableName: CONNECTION_TABLE_NAME,
      Item: {
        connectionId: { S: connectionId },
        ttl: {N: (Math.floor(new Date().getTime() / 1000) + 2 * 60 * 60).toString() }
      }
    };
    await dynamodb.putItem(putParams).promise();
    return {
      statusCode: 200,
      body: 'Connected.'
    };
  } catch (err) {
    console.log('connect', err);
    return {
      statusCode: 500,
      body: JSON.stringify(err)
    };
  }
};

const disconnect = async(event) => {
  try {
    const connectionId = event.requestContext.connectionId;
    const deleteParams = {
      TableName: CONNECTION_TABLE_NAME,
      Key: {
        connectionId: { S: connectionId }
      }
    };
    await dynamodb.deleteItem(deleteParams).promise();
    return {
      statusCode: 200,
      body: 'Disconnected.'
    };
  } catch (err) {
    console.log('disconnect', err);
    return {
      statusCode: 500,
      body: JSON.stringify(err)
    };
  }
};

module.exports = {handleUpdate, connect, disconnect};