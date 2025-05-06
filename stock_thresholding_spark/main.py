from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, lag, abs, expr
import pyspark.sql.functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("Stock Price Analysis").getOrCreate()

# Define the GCS path pattern to read all CSV files
gcs_path = "gs://oppe1_bucket/*.csv"

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

# Define window specification to calculate previous close price per stock
windowSpec = Window.partitionBy("stock_ticker").orderBy("timestamp")

# Calculate percentage change from previous trade
df_with_change = (
    df_sorted.withColumn("prev_close", lag("close", 1).over(windowSpec))
    .filter(col("prev_close").isNotNull())
    .withColumn(
        "pct_change", abs((col("close") - col("prev_close")) / col("prev_close") * 100)
    )
)

# Cache the dataframe since we'll use it multiple times
df_with_change.cache()

# Calculate the exact percentiles using expr and percentile
# Create an array of the percentiles we want to calculate
percentiles_expr = expr(
    "percentile(pct_change, array(0.95, 0.99, 0.995, 0.9995, 0.99995))"
)
percentile_values = df_with_change.select(
    percentiles_expr.alias("percentiles")
).collect()[0][0]

# Extract the percentile values
p95_value = percentile_values[0]
p99_value = percentile_values[1]
p995_value = percentile_values[2]
p9995_value = percentile_values[3]
p99995_value = percentile_values[4]

# Count trades exceeding each percentile threshold
count_exceeding_p95 = df_with_change.filter(col("pct_change") > p95_value).count()
count_exceeding_p99 = df_with_change.filter(col("pct_change") > p99_value).count()
count_exceeding_p995 = df_with_change.filter(col("pct_change") > p995_value).count()
count_exceeding_p9995 = df_with_change.filter(col("pct_change") > p9995_value).count()
count_exceeding_p99995 = df_with_change.filter(col("pct_change") > p99995_value).count()

# Create a dataframe with the results
result_data = [
    ("95th", p95_value, count_exceeding_p95),
    ("99th", p99_value, count_exceeding_p99),
    ("99.5th", p995_value, count_exceeding_p995),
    ("99.95th", p9995_value, count_exceeding_p9995),
    ("99.995th", p99995_value, count_exceeding_p99995),
]

result_df = spark.createDataFrame(
    result_data,
    [
        "Percentile of % change in stock price",
        "Value of % change in stock price",
        "Number of trades exceeding this value",
    ],
)

# Show the results
result_df.show()
result_df.write.csv("output", header=True)
