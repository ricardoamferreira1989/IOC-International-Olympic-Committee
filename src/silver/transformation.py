from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    upper,
    avg,
    round,
)


# ==========================================
# Silver Layer Transformation
# ==========================================

def transform_silver(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
) -> None:
    """
    Read Bronze layer, clean data and create Silver datasets.
    """
    # ==========================================
    # Read Bronze Layer
    # ==========================================

    athlete_events_df = (
        spark.read
        .format("parquet")
        .load(bronze_path)
    )


    # ==========================================
    # Clean Raw Olympic Dataset
    # ==========================================

    # Remove duplicated records
    athlete_events_df = (
        athlete_events_df
        .dropDuplicates()
    )


    # Remove records without athlete ID
    athlete_events_df = (
        athlete_events_df
        .filter(
            col("ID").isNotNull()
        )
    )

    # Standardize text columns
    athlete_events_df = (
        athlete_events_df
        .withColumn(
            "NOC",
            upper(col("NOC"))
        )
    )


    # Replace missing medals
    athlete_events_df = (
        athlete_events_df
        .fillna(
            {"Medal": "NA"}
        )
    )


    # ==========================================
    # Convert Numeric Columns
    # ==========================================

    athlete_events_df = (
        athlete_events_df
        .withColumn(
            "Age",
            col("Age").cast("integer")
        )
        .withColumn(
            "Height",
            col("Height").cast("integer")
        )
        .withColumn(
            "Weight",
            col("Weight").cast("integer")
        )
    )


    # ==========================================
    # Handle Missing Numeric Values
    # Using Column Mean Imputation
    # ==========================================

    age_mean = (
        athlete_events_df
        .select(avg("Age"))
        .first()[0]
    )

    height_mean = (
        athlete_events_df
        .select(avg("Height"))
        .first()[0]
    )

    weight_mean = (
        athlete_events_df
        .select(avg("Weight"))
        .first()[0]
    )


    athlete_events_df = (
        athlete_events_df
        .fillna(
            {
                "Age": round(age_mean),
                "Height": round(height_mean),
                "Weight": round(weight_mean),
            }
        )
    )


    # ==========================================
    # Create Athlete Silver Dataset
    # ==========================================

    athlete_df = (
        athlete_events_df
        .select(
            "ID",
            "Name",
            "Sex",
            "Age",
            "Height",
            "Weight",
            "Team",
            "NOC"
        )
        .dropDuplicates()
    )


    (
        athlete_df.write
        .format("parquet")
        .mode("overwrite")
        .save(
            f"{silver_path}/athlete_clean"
        )
    )


    # ==========================================
    # Create Event Silver Dataset
    # ==========================================

    event_df = (
        athlete_events_df
        .select(
            "Event",
            "Sport",
        )
        .dropDuplicates()
    )


    (
        event_df.write
        .format("parquet")
        .mode("overwrite")
        .save(
            f"{silver_path}/event_clean"
        )
    )

    # ==========================================
    # Create Game Silver Dataset
    # ==========================================

    game_df = (
        athlete_events_df
        .select(
            "Year",
            "Season",
            "City",
        )
        .dropDuplicates()
    )


    (
        game_df.write
        .format("parquet")
        .mode("overwrite")
        .save(
            f"{silver_path}/game_clean"
        )
    )