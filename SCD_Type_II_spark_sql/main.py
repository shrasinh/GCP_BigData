from pyspark.sql import SparkSession


def implement_scd_type2_sql(
    source_path,
    target_path,
    business_keys,
    tracked_columns,
    unique_id_col,
    start_date_col="effective_start_date",
    end_date_col="effective_end_date",
):
    """
    Generic SCD Type 2 implementation using SparkSQL, reading from and writing to CSV files.

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
    spark = SparkSession.builder.appName(
        "SCD Type II Implementation - SQL"
    ).getOrCreate()

    # Read source and target CSV files and register them as temporary tables
    spark.read.csv(source_path, header=True, inferSchema=True).createOrReplaceTempView(
        "source_table"
    )
    spark.read.csv(target_path, header=True, inferSchema=True).createOrReplaceTempView(
        "target_table"
    )

    # Define SQL components
    business_keys_sql = ", ".join([f"t.{key} = s.{key}" for key in business_keys])
    business_keys_select = ", ".join([f"s.{key}" for key in business_keys])
    target_columns_sql = ", ".join([col for col in spark.table("target_table").columns])
    source_columns_sql = ", ".join(
        [f"s.{col}" for col in spark.table("source_table").columns]
    )
    target_columns_sql_without_end_date = ", ".join(
        [
            f"t.{col}"
            for col in spark.table("target_table").columns
            if col != end_date_col
        ]
    )

    # Find the max ID
    max_id = spark.sql(
        f"""SELECT COALESCE(MAX({unique_id_col}), 0) as max_id FROM target_table"""
    ).collect()[0]["max_id"]

    # 1. Create view for active records
    spark.sql(
        f"""
    CREATE OR REPLACE TEMPORARY VIEW active_records AS
    SELECT * FROM target_table t
    WHERE t.{end_date_col} = '9999-12-31'
    """
    )

    # 2. Identify changed records
    spark.sql(
        f"""
    CREATE OR REPLACE TEMPORARY VIEW changed_records AS
    SELECT t.*
    FROM active_records t
    JOIN source_table s ON {business_keys_sql}
    WHERE {' OR '.join([f't.{col} <> s.{col}' for col in tracked_columns])}
    """
    )

    # 3. Identify deleted records
    spark.sql(
        f"""
    CREATE OR REPLACE TEMPORARY VIEW deleted_records AS
    SELECT {target_columns_sql_without_end_date}, CURRENT_TIMESTAMP() as {end_date_col}
    FROM active_records t
    LEFT ANTI JOIN source_table s ON {business_keys_sql}
    """
    )

    # 4. Create view for unchanged records
    spark.sql(
        f"""
    CREATE OR REPLACE TEMPORARY VIEW unchanged_records AS
    SELECT t.*
    FROM target_table t
    LEFT ANTI JOIN (
        SELECT {business_keys_select.replace('s.', '')} FROM changed_records
        UNION
        SELECT {business_keys_select.replace('s.', '')} FROM deleted_records
    ) combined ON {business_keys_sql.replace('s.', 'combined.')}
    """
    )

    # 5. Expire changed records
    spark.sql(
        f"""
    CREATE OR REPLACE TEMPORARY VIEW records_to_expire AS
    SELECT {target_columns_sql_without_end_date}, CURRENT_TIMESTAMP() as {end_date_col}
    FROM active_records t
    JOIN changed_records c ON {business_keys_sql.replace('s.', 'c.')}
    """
    )

    # 6. Create new records for changes
    spark.sql(
        f"""
    CREATE OR REPLACE TEMPORARY VIEW new_changed_records AS
    SELECT
        {source_columns_sql},
        CURRENT_TIMESTAMP() as {start_date_col},
        CAST('9999-12-31' AS TIMESTAMP) as {end_date_col},
        (ROW_NUMBER() OVER (ORDER BY {business_keys_select}) + {max_id}) AS {unique_id_col}
    FROM source_table s
    JOIN changed_records c ON {business_keys_sql.replace('t.', 'c.')}
    """
    )

    # Count the new changed records to update max_id
    new_changed_count = spark.sql(
        "SELECT COUNT(*) as count FROM new_changed_records"
    ).collect()[0]["count"]
    max_id += new_changed_count

    # 7. Identify new records
    spark.sql(
        f"""
    CREATE OR REPLACE TEMPORARY VIEW new_records AS
    SELECT
        {source_columns_sql},
        CURRENT_TIMESTAMP() as {start_date_col},
        CAST('9999-12-31' AS TIMESTAMP) as {end_date_col},
        (ROW_NUMBER() OVER (ORDER BY {business_keys_select}) + {max_id}) AS {unique_id_col}
    FROM source_table s
    LEFT ANTI JOIN target_table t ON {business_keys_sql}
    """
    )

    # 8. Union all records together

    final_df = spark.sql(
        f"""
    SELECT * FROM unchanged_records
    UNION ALL 
    SELECT {target_columns_sql} FROM records_to_expire
    UNION ALL 
    SELECT {target_columns_sql} FROM new_changed_records
    UNION ALL 
    SELECT {target_columns_sql} FROM new_records
    UNION ALL 
    SELECT {target_columns_sql} FROM deleted_records
    """
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
    result_df = implement_scd_type2_sql(
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
