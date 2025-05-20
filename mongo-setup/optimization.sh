#!/bin/bash
# MongoDB Index Optimization Script
# Run this after initial setup to optimize MongoDB performance

echo "Waiting for MongoDB to be fully initialized..."
sleep 30  # Give time for MongoDB services to be fully available

echo "Optimizing MongoDB indexes and configuration..."
mongo --host mongo-router:27017 -u $MONGO_ROOT_USERNAME -p $MONGO_ROOT_PASSWORD --authenticationDatabase admin <<EOF
// Configure chunk size for better distribution (default is 64MB)
use config
db.settings.updateOne(
   { _id: "chunksize" },
   { \$set: { value: 32 } },
   { upsert: true }
)
echo "Set optimal chunk size to 32MB"

// Create indexes for technical indicators and query optimization
use finance_data

// Optimize company_info collection
db.company_info.createIndex({ "sector": 1 })
db.company_info.createIndex({ "industry": 1 })
db.company_info.createIndex({ "marketCap": -1 })
db.company_info.createIndex({ "ticker": 1, "sector": 1 })

// Function to optimize stock collections with technical indicators
function optimizeStockCollection(symbol) {
  collName = "stock_" + symbol;

  // Only proceed if collection exists
  if (db.getCollectionNames().indexOf(collName) > -1) {
    // Add indexes for time-based queries with specific fields
    // For technical indicators
    db[collName].createIndex({ "date": 1, "close": 1 })
    db[collName].createIndex({ "date": 1, "volume": 1 })
    
    // For technical indicator queries
    db[collName].createIndex({ "date": 1, "sma_20": 1 })
    db[collName].createIndex({ "date": 1, "rsi": 1 })
    db[collName].createIndex({ "date": 1, "macd": 1 })
    
    // For volatility analysis
    db[collName].createIndex({ "date": 1, "bb_upper": 1, "bb_lower": 1 })
    
    // For correlation queries
    db[collName].createIndex({ "date": 1, "ticker": 1, "close": 1, "volume": 1 })
    
    // Compound indexes for technical analysis
    db[collName].createIndex({ "rsi": 1, "date": -1 })
    db[collName].createIndex({ "macd": 1, "date": -1 })
    
    // TTL index for automatic data cleanup (if needed)
    // db[collName].createIndex({ "date": 1 }, { expireAfterSeconds: 180 * 24 * 60 * 60 }) // 180 days
    
    console.log("Optimized " + collName + " with technical indicator indexes")
  }
  
  // Optimize actions collection
  actionsCollName = "stock_" + symbol + "_actions";
  if (db.getCollectionNames().indexOf(actionsCollName) > -1) {
    // Add specialized indexes for dividend information
    db[actionsCollName].createIndex({ "date": 1, "dividends": 1 })
    db[actionsCollName].createIndex({ "dividends": -1 }) // For finding highest dividends
    console.log("Optimized " + actionsCollName + " for dividend analysis")
  }
}

// Optimize predictions collection
db.predictions.createIndex({ "target_date": 1, "predicted_price": 1 })
db.predictions.createIndex({ "prediction_date": 1, "accuracy_percent": -1 }) // For finding most accurate predictions
db.predictions.createIndex({ "symbol": 1, "accuracy_percent": -1 }) // For finding best predicted symbols
db.predictions.createIndex({ "symbol": 1, "target_date": 1, "predicted_price": 1, "actual_price": 1 }) // For comprehensive analysis

// Create a time-series view collection for time-based prediction analysis
db.createCollection("prediction_accuracy_timeseries", {
  viewOn: "predictions",
  pipeline: [
    { \$match: { actual_price: { \$ne: null } } },
    { \$project: {
        symbol: 1,
        prediction_date: 1,
        target_date: 1,
        accuracy_percent: { 
          \$multiply: [
            100,
            { \$subtract: [
              1, 
              { \$abs: { \$divide: [
                { \$subtract: ["\$predicted_price", "\$actual_price"] }, 
                "\$actual_price"
              ]}}
            ]}
          ]
        }
      }
    }
  ]
})
console.log("Created prediction_accuracy_timeseries view for analytics")

// Run optimization for each symbol
var symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"];
symbols.forEach(optimizeStockCollection);

// Add wildcard index for flexible queries on symbols we might add later
// Note: This requires MongoDB 4.2+
db.predictions.createIndex({ "symbol.$**": 1 })

// Create a collection for analytics summaries if it doesn't exist
if (db.getCollectionNames().indexOf("analytics_summary") === -1) {
  db.createCollection("analytics_summary")
  db.analytics_summary.createIndex({ "date": 1 }, { unique: true })
  db.analytics_summary.createIndex({ "most_accurate_model": 1 })
  console.log("Created analytics_summary collection for model performance tracking")
}

// Verify sharding status
sh.status()
EOF

echo "MongoDB optimization completed!"

# Create a cron job to run maintenance tasks
echo "Setting up MongoDB maintenance cron job..."
cat > /tmp/mongodb_maintenance <<EOF
#!/bin/bash
# MongoDB maintenance script

echo "Running MongoDB maintenance tasks at \$(date)"
mongo --host mongo-router:27017 -u \$MONGO_ROOT_USERNAME -p \$MONGO_ROOT_PASSWORD --authenticationDatabase admin <<MONGOSCRIPT
// Compact collections
use finance_data
db.runCommand({ compact: "predictions" })

// Check index usage and remove unused indexes
var collections = db.getCollectionNames()
collections.forEach(function(collName) {
  if (collName.startsWith("stock_")) {
    var indexStats = db[collName].aggregate([
      { \$indexStats: {} },
      { \$match: { "accesses.ops": { \$lt: 10 } } }
    ]).toArray()
    
    // Log unused indexes but don't automatically remove them
    if (indexStats.length > 0) {
      print("Collection " + collName + " has " + indexStats.length + " unused indexes")
      indexStats.forEach(function(stat) {
        print("  Index: " + JSON.stringify(stat.name) + ", ops: " + stat.accesses.ops)
      })
    }
  }
})

// Update chunk distribution statistics
sh.status({ verbose: true })
MONGOSCRIPT

echo "MongoDB maintenance completed at \$(date)"
EOF

# Add to crontab if it's a production environment
# (0 2 * * * /tmp/mongodb_maintenance) | crontab -

# Add the script to the system but don't schedule it automatically
chmod +x /tmp/mongodb_maintenance
echo "Created MongoDB maintenance script at /tmp/mongodb_maintenance"
echo "In production, consider adding it to crontab to run daily at 2 AM:"
echo "0 2 * * * /tmp/mongodb_maintenance"
