from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("Stock Price Analysis").getOrCreate()

# Define the GCS path pattern to read all CSV files
gcs_path = "gs://oppe_2/*.csv"

# Read the CSV files
df = spark.read.option("header", "true").option("inferSchema", "true").csv(gcs_path)

# Clean the data - handle bad rows
df_clean = df.filter(
    col("timestamp").isNotNull()
    & col("close").isNotNull()
    & col("open").isNotNull()
    & col("high").isNotNull()
    & col("low").isNotNull()
    & col("volume").isNotNull()
)

# Convert string columns to appropriate types if needed
df_clean = (
    df_clean.withColumn("timestamp", col("timestamp").cast("timestamp"))
    .withColumn("close", col("close").cast("double"))
    .withColumn("open", col("open").cast("double"))
    .withColumn("high", col("high").cast("double"))
    .withColumn("low", col("low").cast("double"))
    .withColumn("volume", col("volume").cast("long"))
)

# Extract stock ticker from filename
df_clean = df_clean.withColumn("stock_ticker", F.input_file_name())

# Sort data by stock ticker and timestamp to ensure correct calculation of price changes
df_sorted = df_clean.orderBy("stock_ticker", "timestamp")

df_sorted.selectExpr(
    "stock_ticker AS key",
    "to_json(struct(*)) AS value"
).write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "104.154.133.10:9092") \
    .option("topic", "stock-trades") \
    .save()

print("Data successfully loaded to Kafka")