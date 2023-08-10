
import { request, gql, GraphQLClient } from 'graphql-request';
// import entire SDK
import AWS from 'aws-sdk';


// Configure AWS SDK with the credentials created before
// You should probably use a .env file for this
AWS.config.update({
    region: "ap-southeast-2",
    accessKeyId: "AKIA3CLAIV37UW3SL4OP",
    secretAccessKey: "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"
})

// Create a kinesis client
const kinesisClient = new AWS.Kinesis()

// The stream name for the data stream we created
const KINESIS_STREAM_NAME = 'blockchain_stream'



const yelpApiUrl = "https://api.studio.thegraph.com/query/46657/warpped-ether-dashboard/v0.0.1";


let delay = 0;

const graphQLClient = new GraphQLClient(yelpApiUrl, {

});

const query = gql`
query getTransfers($timestamp_gt: String, $timestamp_lt: String) {
  transfers(where: {blockTimestamp_gt: $timestamp_gt, blockTimestamp_lt: $timestamp_lt})
  {
    id
    from
    to
    value
    blockTimestamp
  }
}
`;

var counter = 1;

async function intervalFunc() {
  console.log("Counter ::: "+counter);
  const variables = {
    // timestamp_gt: (Math.round(Date.now() / 1000) -  (counter * 24 * 60 * 60)).toString(),
    // timestamp_lt: (Math.round(Date.now() / 1000) - (--counter * 24 * 60 * 60)).toString()
    timestamp_gt: (Math.round(Date.now() / 1000) - (1 * 24 * 60 * 60)).toString(),
    timestamp_lt: (Math.round(Date.now() / 1000)).toString()
  };
  console.log("GraphQL Request to sent @ ",new Date());
  console.log("GraphQL Request variables ",variables);
  const response = await graphQLClient.request(query, variables);
  var obj = response.transfers;
  console.log("Graph QL Response Transfers ::: ", obj.length);
  obj.forEach(function(row, index) { // we add index param here, starts with 0
    setTimeout(function() {
      console.log("Row Start *************************************");
      console.log(row);
      sendTokinesisStream (row);
      console.log("Row End ****************************************");
    }, 5000) // or just index, depends on your needs 5000*(index+1)
          
  })

}

const sendTokinesisStream = (row) => {
  kinesisClient.putRecord(
    {
      Data: JSON.stringify(row),
      StreamName: KINESIS_STREAM_NAME,
      PartitionKey: 'id'
    },
    (err, data) => {
      if (err) {
        throw err
      }
      console.log(data)
    }
  )
}

setInterval(intervalFunc, 24 * 60 * 60 * 1000);
