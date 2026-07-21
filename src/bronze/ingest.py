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
    Read the raw Olympic dataset and store it in the Bronze layer.
    """

    athlete_events_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(input_path)
    )

    if athlete_events_df.rdd.isEmpty():
        raise ValueError("The input dataset is empty.")

    (
        athlete_events_df.write
        .format("parquet")
        .mode("overwrite")
        .save(output_path)
    )

    return athlete_events_df