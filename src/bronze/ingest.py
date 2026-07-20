from pyspark.sql import SparkSession, DataFrame


# ==========================================
# Bronze Layer Ingestion
# ==========================================

def ingest_bronze(
    spark: SparkSession,
    input_path: str,
    output_path: str,
) -> DataFrame:
    """
    Read raw Olympic dataset and store it in the Bronze.
    """
    # ==========================================
    # Read Raw Olympic Dataset
    # ==========================================

    athlete_events_df = (
        spark.read
        .format("csv")
        .option("inferSchema", True)
        .option("header", True)
        .load(input_path)
    )


    # ==========================================
    # Store Data in Bronze Layer
    # ==========================================

    (
        athlete_events_df.write
        .format("parquet")
        .mode("overwrite")
        .save(output_path)
    )


    return athlete_events_df