'use strict';

const AWS = require('aws-sdk');
const Athena = new AWS.Athena({});
const DynamoDB = new AWS.DynamoDB({});
const retry = require('async-retry');

const WEBSOCKET_ENDPOINT = process.env.WEBSOCKET_ENDPOINT;
const MANAGEMENT_ENDPOINT = WEBSOCKET_ENDPOINT.replace('wss', 'https');
const CONNECTION_TABLE_NAME = process.env.CONNECTION_TABLE_NAME;
const DATABASE_NAME = process.env.DATABASE_NAME;
const WORKGROUP_NAME = process.env.WORKGROUP_NAME;
const TABLE_NAME = "raw";

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

const queryAthena = async (query, formatter) =>  {
  // start athena query
  let exec = Athena.startQueryExecution({
    QueryString: query,
    WorkGroup: WORKGROUP_NAME
  }).promise();

  // wait query done (polling the query execution state using retry)
  exec = exec.then(async (data) => {
    const params = {
      QueryExecutionId: data.QueryExecutionId
    };
    return retry(async bail => {
        let data = await Athena.getQueryExecution(params).promise();
        console.log(data);
        if(data && data.QueryExecution && data.QueryExecution.Status) {
          if (data.QueryExecution.Status.State == "SUCCEEDED") {
            return params;
          } else if (data.QueryExecution.Status.State == "FAILED") {
            bail(new Error(data.QueryExecution.Status.StateChangeReason));
          } else {
            throw new Error("Query is not done. Retry.");
          }
        } else {
          throw new Error("Query is not done. Retry.");
        }
      },
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

  result.ResultSet.Rows.shift(); // 先頭はカラム名が入っているので除く
  let values = result.ResultSet.Rows.map(row => formatter(row.Data));
  let nextToken = result.NextToken; // if not null, it has next page
  while(nextToken) {
    result = await Athena.getQueryResults({ NextToken: nextToken , ...params }).promise();
    if(!(result.ResultSet && result.ResultSet.Rows)) break;
    result.ResultSet.Rows.shift(); // 先頭はカラム名が入っているので除く
    values = values.concat(result.ResultSet.Rows.shift().map(row => formatter(row.Data)));
    nextToken = result.NextToken;
  }
  return values;
}

const aggregateProcessedData = async () => {
  const query = `
  SELECT B.track_id, track_name, artist_name, activity_type, cnt
  FROM
      ${DATABASE_NAME}.reference_data AS A,
      (SELECT track_id, activity_type, COUNT(device_id) as cnt
      FROM ${DATABASE_NAME}.raw
      GROUP BY track_id, activity_type
      ORDER BY track_id, activity_type)  AS B
  WHERE
      A.track_id = B.track_id
  ORDER BY
      track_id, activity_type
  `;

  const formatter = (row) => [
    parseInt(row[0].VarCharValue), // track_id
    row[1].VarCharValue,           // track_name
    row[2].VarCharValue,           // artist_name
    row[3].VarCharValue,           // activity_type
    parseInt(row[4].VarCharValue), // COUNT(device_id)
  ];

  return queryAthena(query, formatter);
}

const handleUpdate = async (event) => {
  console.log(event);
  try {
    let payload = null;
    if(event['source'] == 'aws.glue' && event['detail-type'] == 'Glue Data Catalog Table State Change') {
      if(event['detail']['tableName'] == TABLE_NAME) {
        payload = { data: await aggregateProcessedData(), type: 'batch', time: new Date().getTime() };
      }
    }
    
    if(payload != null) {
      console.log(payload);
      let connectionData = await DynamoDB.scan({ TableName: CONNECTION_TABLE_NAME, ProjectionExpression: 'connectionId' }).promise();
      let promises = [];
      let deletePromises = [];
      connectionData.Items.map(async (item) => {
        promises.push(sendMessageToClient(MANAGEMENT_ENDPOINT, item.connectionId.S, payload));
      });

      try {
        await Promise.all(promises);
      } catch (err) {
        if (err instanceof ConnectionGoneError) {
          const deleteParams = {
            TableName: CONNECTION_TABLE_NAME,
            Key: {
              connectionId: { S: err.connectionId }
            }
          };
          deletePromises.push(DynamoDB.deleteItem(deleteParams).promise()); 
        }
        // wait remove invalid connections
        if (deletePromises.length != 0) {
          try {
            await Promise.all(deletePromises);
          } catch (err) {
            console.log(err);
          }
        }
      }
    }
    return {
      statusCode: 200
    };
  } catch (err) {
    console.log("Error:", err);
    return {
        statusCode: 500
    };
  }
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
    await DynamoDB.putItem(putParams).promise();
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
    await DynamoDB.deleteItem(deleteParams).promise();
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