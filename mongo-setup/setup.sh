#!/bin/bash
# MongoDB Sharded Cluster Setup Script

echo "Waiting for MongoDB services to start..."
sleep 30  # Give time for MongoDB containers to initialize

echo "Initializing config server replica set..."
mongo --host mongo-config1:27017 -u $MONGO_ROOT_USERNAME -p $MONGO_ROOT_PASSWORD --authenticationDatabase admin <<EOF
rs.initiate(
  {
    _id: "configrs",
    configsvr: true,
    members: [
      { _id: 0, host: "mongo-config1:27017" },
      { _id: 1, host: "mongo-config2:27017" },
      { _id: 2, host: "mongo-config3:27017" }
    ]
  }
)
EOF

sleep 10  # Wait for replica set to initialize

echo "Initializing shard 1 replica set..."
mongo --host mongo-shard1:27017 -u $MONGO_ROOT_USERNAME -p $MONGO_ROOT_PASSWORD --authenticationDatabase admin <<EOF
rs.initiate(
  {
    _id: "shard1rs",
    members: [
      { _id: 0, host: "mongo-shard1:27017" }
    ]
  }
)
EOF

echo "Initializing shard 2 replica set..."
mongo --host mongo-shard2:27017 -u $MONGO_ROOT_USERNAME -p $MONGO_ROOT_PASSWORD --authenticationDatabase admin <<EOF
rs.initiate(
  {
    _id: "shard2rs",
    members: [
      { _id: 0, host: "mongo-shard2:27017" }
    ]
  }
)
EOF

sleep 20  # Wait for replica sets to initialize

echo "Adding shards to the cluster..."
mongo --host mongo-router:27017 -u $MONGO_ROOT_USERNAME -p $MONGO_ROOT_PASSWORD --authenticationDatabase admin <<EOF
sh.addShard("shard1rs/mongo-shard1:27017")
sh.addShard("shard2rs/mongo-shard2:27017")
EOF

echo "Enabling sharding for the finance_data database..."
mongo --host mongo-router:27017 -u $MONGO_ROOT_USERNAME -p $MONGO_ROOT_PASSWORD --authenticationDatabase admin <<EOF
use admin
sh.enableSharding("finance_data")

// Create indexes for each collection and shard by symbol
use finance_data
db.createCollection("company_info")
db.company_info.createIndex({ "ticker": 1 })
sh.shardCollection("finance_data.company_info", { "ticker": 1 })

// For stock data collections, we'll use a function to set them up dynamically
function setupStockCollection(symbol) {
  // For OHLCV data
  collName = "stock_" + symbol;
  db.createCollection(collName);
  db[collName].createIndex({ "date": 1 });
  db[collName].createIndex({ "ticker": 1, "date": 1 });
  sh.shardCollection("finance_data." + collName, { "ticker": 1, "date": 1 });
  
  // For actions data
  actionsCollName = "stock_" + symbol + "_actions";
  db.createCollection(actionsCollName);
  db[actionsCollName].createIndex({ "date": 1 });
  db[actionsCollName].createIndex({ "ticker": 1, "date": 1 });
  sh.shardCollection("finance_data." + actionsCollName, { "ticker": 1, "date": 1 });
}

// Setup collections for common stock symbols
var symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"];
symbols.forEach(setupStockCollection);

// Create predictions collection
db.createCollection("predictions");
db.predictions.createIndex({ "symbol": 1, "prediction_date": 1, "target_date": 1 });
sh.shardCollection("finance_data.predictions", { "symbol": 1, "prediction_date": 1 });

EOF

echo "MongoDB sharded cluster setup completed!"
