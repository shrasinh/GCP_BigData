from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


def implement_scd_type2(
    source_path,
    target_path,
    business_keys,
    tracked_columns,
    unique_id_col,
    start_date_col="effective_start_date",
    end_date_col="effective_end_date",
):
    """
    Generic SCD Type 2 implementation reading from and writing to CSV files.

    Parameters
    -----------
    source_path : str
        Path to source CSV file
    target_path : str
        Path to target CSV file
    business_keys : list
        List of columns that uniquely identify a business entity
    tracked_columns : list
        List of columns to track changes (excluding business keys)
    unique_id_col : str
        Name of the auto-incrementing unique identifier column
    start_date_col : str
        Column name for effective start date
    end_date_col : str
        Column name for effective end date

    Returns
    --------
    DataFrame
        Updated target table with SCD Type 2 implementation
    """

    # Initialize Spark Session
    spark = SparkSession.builder.appName("SCD Type II Implementation").getOrCreate()

    # Read source and target CSV files
    source_df = spark.read.csv(source_path, header=True, inferSchema=True)
    target_df = spark.read.csv(target_path, header=True, inferSchema=True)

    # Get the maximum existing record_id
    max_id = target_df.agg(max(col(unique_id_col))).collect()[0][0]

    # Getting the current timestamp
    curr_timestamp = current_timestamp()

    # Getting the max timestamp
    max_date = lit("9999-12-31").cast("timestamp")

    # Create a combined condition for all tracked columns
    change_condition = None
    for column in tracked_columns:
        if change_condition is None:
            change_condition = target_df[column] != source_df[column]
        else:
            change_condition = change_condition | (
                target_df[column] != source_df[column]
            )

    # Getting the currently active records of target table
    active_records = target_df.filter(col(end_date_col) == max_date)

    # 1. Find changed records
    changed_records = active_records.join(source_df, business_keys, "inner").filter(
        change_condition
    )

    # 2. Identify deleted records
    deleted_records = active_records.join(
        source_df.select(*business_keys), business_keys, "leftanti"
    ).withColumn(end_date_col, curr_timestamp)

    # 3. Get unchanged current records
    unchanged_records = target_df.join(
        changed_records.select(*business_keys).unionByName(
            deleted_records.select(*business_keys)
        ),
        business_keys,
        "leftanti",
    )

    # 4. Expire current records that have changes
    records_to_expire = active_records.join(
        changed_records.select(*business_keys), business_keys, "inner"
    ).withColumn(end_date_col, curr_timestamp)

    # 5. Create new records for changed data
    w = Window.orderBy(monotonically_increasing_id())
    new_changed_records = (
        source_df.join(changed_records.select(*business_keys), business_keys, "inner")
        .select(
            *source_df.columns,
            curr_timestamp.alias(start_date_col),
            max_date.alias(end_date_col)
        )
        .withColumn(unique_id_col, row_number().over(w) + max_id)
    )

    # Update max_id for new records
    max_id = max_id + new_changed_records.count()

    # 6. Identify completely new records
    new_records = (
        source_df.join(target_df.select(*business_keys), business_keys, "leftanti")
        .select(
            *source_df.columns,
            curr_timestamp.alias(start_date_col),
            max_date.alias(end_date_col)
        )
        .withColumn(unique_id_col, row_number().over(w) + max_id)
    )

    # 7. Union all records together
    final_df = (
        unchanged_records.unionByName(records_to_expire)
        .unionByName(new_changed_records)
        .unionByName(new_records)
        .unionByName(deleted_records)
    )

    return final_df


if __name__ == "__main__":
    # Define paths
    SOURCE_PATH = "source.csv"
    TARGET_PATH = "target.csv"
    OUTPUT_PATH = "target_updated"

    # Define business keys and tracked columns
    business_keys = ["customer_id"]
    tracked_columns = ["name", "city"]

    # Execute SCD Type 2
    result_df = implement_scd_type2(
        source_path=SOURCE_PATH,
        target_path=TARGET_PATH,
        unique_id_col="record_id",
        business_keys=business_keys,
        tracked_columns=tracked_columns,
    )

    # Show results
    result_df.orderBy("record_id").show(truncate=False)
    # Write the results to CSV
    result_df.coalesce(1).write.mode("overwrite").csv(OUTPUT_PATH, header=True)
