from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, when, lag, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from google.cloud import pubsub_v1
import json

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("StockAnomalyDetection-Improved") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Define schema for stock data
stock_schema = StructType([
    StructField("stock_ticker", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True)
])

# Read from Kafka
raw_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "104.154.133.10:9092") \
    .option("subscribe", "stock-trades") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 500) \
    .load()

# Parse JSON from Kafka
parsed_stream = raw_stream \
    .select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), stock_schema).alias("data")
    ) \
    .select("key", "data.*")

# Add watermark to handle late data
watermarked_stream = parsed_stream \
    .withWatermark("timestamp", "15 minutes")

# Initialize Google PubSub publisher client
PROJECT_ID = "celtic-guru-448518-f8"
TOPIC_NAME = "StockVolumeAnomalies"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

def publish_to_pubsub(stock_ticker, timestamp, volume, avg_volume, volume_change_pct):
    """Publish volume anomaly alert to PubSub"""
    message = {
        "stock_ticker": stock_ticker,
        "timestamp": str(timestamp),
        "volume": volume,
        "avg_volume": avg_volume,
        "volume_change_pct": volume_change_pct,
        "warning": f"Traded Volume more than 2% of its average"
    }
    
    # Convert the message to JSON and encode as bytes
    message_data = json.dumps(message).encode('utf-8')
    
    # Publish the message
    future = publisher.publish(topic_path, data=message_data)
    return future

# Process each micro-batch with foreachBatch to apply custom anomaly detection logic
def process_batch(batch_df, batch_id):
    # Skip if batch is empty
    if batch_df.count() == 0:
        return
    
    # Calculate metrics per stock ticker
    stock_data = batch_df.select(
        "stock_ticker", "timestamp", "open", "high", "low", "close", "volume"
    ).orderBy("stock_ticker", "timestamp")
    
    # If we have data, perform anomaly detection
    if stock_data.count() > 0:
        # Define window for previous minute's close price
        windowSpec = Window.partitionBy("stock_ticker").orderBy("timestamp")
        
        # Calculate previous minute's close price for A1 check
        with_prev_close = stock_data \
            .withColumn("prev_minute_close", lag("close", 1).over(windowSpec))
        
        # Calculate 10-minute average volume for A2 check
        volume_window = Window.partitionBy("stock_ticker") \
                             .orderBy("timestamp") \
                             .rowsBetween(-10, -1)  # Last 10 minutes, excluding current minute
        
        with_metrics = with_prev_close \
            .withColumn("avg_10min_volume", avg("volume").over(volume_window))
        
        # Apply anomaly detection rules:
        # A1: Current trade price deviates from previous minute's close by >0.5%
        # A2: Volume is >2% above the 10-minute average volume
        anomaly_df = with_metrics \
            .withColumn("price_change_pct", 
                       when(col("prev_minute_close").isNotNull(), 
                            (col("close") - col("prev_minute_close")) / col("prev_minute_close") * 100)
                       .otherwise(lit(0))) \
            .withColumn("volume_change_pct", 
                       when(col("avg_10min_volume").isNotNull() & (col("avg_10min_volume") > 0),
                            (col("volume") - col("avg_10min_volume")) / col("avg_10min_volume") * 100)
                       .otherwise(lit(0))) \
            .withColumn("is_price_anomaly", 
                       when(col("prev_minute_close").isNotNull(), 
                            F.abs(col("price_change_pct")) > 0.5)
                       .otherwise(lit(False))) \
            .withColumn("is_volume_anomaly", 
                       when(col("avg_10min_volume").isNotNull(), 
                            col("volume_change_pct") > 2.0)
                       .otherwise(lit(False))) \
            .withColumn("anomaly_type",
                       when(col("is_price_anomaly") & col("is_volume_anomaly"), lit("Price+Volume"))
                       .when(col("is_price_anomaly"), lit("Price"))
                       .when(col("is_volume_anomaly"), lit("Volume"))
                       .otherwise(lit("None"))) \
            .withColumn("is_anomaly", 
                       col("is_price_anomaly") | col("is_volume_anomaly"))
        
        # Filter for anomalies only and display
        anomalies = anomaly_df.filter(col("is_anomaly") == True)
        
        # Find volume anomalies (A2 type) and publish to PubSub
        volume_anomalies = anomalies.filter(col("is_volume_anomaly") == True)
        
        # If we have volume anomalies, publish them to PubSub
        if volume_anomalies.count() > 0:
            print(f"Publishing {volume_anomalies.count()} volume anomalies to PubSub")
            
            # Collect the anomalies to process in the driver
            volume_anomalies_list = volume_anomalies.select(
                "stock_ticker", "timestamp", "volume", 
                "avg_10min_volume", "volume_change_pct"
            ).collect()
            
            # Publish each volume anomaly to PubSub
            for row in volume_anomalies_list:
                publish_to_pubsub(
                    row["stock_ticker"],
                    row["timestamp"],
                    row["volume"],
                    row["avg_10min_volume"],
                    row["volume_change_pct"]
                )

        if anomalies.count() > 0:
            print(f"Batch ID: {batch_id} - Found {anomalies.count()} anomalies")
            anomalies.select(
                "stock_ticker", "timestamp", "close", "volume", "prev_minute_close", 
                "avg_10min_volume", "price_change_pct", "volume_change_pct", "anomaly_type"
            ).show(truncate=False)

# Start the streaming query with foreachBatch
streaming_query = watermarked_stream \
    .writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .trigger(processingTime="1 minute") \
    .start()

# Wait for termination
streaming_query.awaitTermination()