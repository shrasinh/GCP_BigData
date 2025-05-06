from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf

# Create SparkConf and set configuration
conf = SparkConf().set("spark.sql.execution.useObjectHashAggregateExec", "true")

# Create Spark Session with the configuration
spark = (
    SparkSession.builder.appName("TimeIntervalClickCounter")
    .config(conf=conf)
    .getOrCreate()
)

# Define schema for the input data
schema = StructType(
    [
        StructField("Date", StringType(), True),
        StructField("Time", StringType(), True),
        StructField("UserID", StringType(), True),
    ]
)

# Read the input file
df = spark.read.csv("input.txt", sep="\t", header=True, schema=schema)


# Define the hash function
def hash_time(time):
    hour = int(time.split(":")[0])
    if 0 <= hour < 6:
        return "0-6"
    elif 6 <= hour < 12:
        return "6-12"
    elif 12 <= hour < 18:
        return "12-18"
    else:
        return "18-24"


# Register the UDF
hash_time_udf = udf(hash_time, StringType())

# Apply the hash function and count occurrences
df_hashed = df.withColumn("time_range", hash_time_udf(col("Time")))
df_counted = df_hashed.groupBy("time_range").count()

# Show results
df_counted.show()
df_counted.explain(True)

# Save results
df_counted.write.mode("overwrite").csv("output")

# Stop Spark session
spark.stop()
