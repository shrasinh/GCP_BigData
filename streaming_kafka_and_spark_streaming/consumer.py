from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "34.58.97.69:9092"
KAFKA_TOPIC = "file-count-line-topic"


def create_spark_session():
    """Creates and returns a Spark session configured for streaming."""
    return SparkSession.builder.appName("KafkaSparkStreamingConsumer").getOrCreate()


def process_stream(spark):
    """Sets up and processes the streaming data from Kafka."""
    # Create a DataFrame representing the stream of input from Kafka
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Convert the binary data from Kafka into string
    value_df = df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Process the data with a 10-second window, sliding every 5 seconds
    windowedCounts = value_df.groupBy(
        window(col("timestamp"), "10 seconds", "5 seconds")
    ).count()

    # Start the streaming query to console output
    query = (
        windowedCounts.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    # Return the query for the caller to manage
    return query


def main():
    # Create Spark session
    spark = create_spark_session()

    # Set log level to reduce noise in console
    spark.sparkContext.setLogLevel("ERROR")

    print("Starting Spark Streaming Consumer...")

    # Process the stream
    query = process_stream(spark)

    try:
        # Wait for the streaming to finish
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping the streaming query...")
        query.stop()

    print("Spark Streaming Consumer stopped.")


if __name__ == "__main__":
    main()
