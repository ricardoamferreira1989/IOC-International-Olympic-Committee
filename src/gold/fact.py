from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    current_timestamp,
    lit,
)


# ==========================================
# Gold Fact Layer
# ==========================================

def create_fact(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
) -> None:
    """
    Create Gold fact table from dimensions
    and Silver data.
    """

    # ==========================================
    # Read  Dimensions
    # ==========================================

    dim_athlete_df = (
        spark.read
        .format("parquet")
        .load(
            f"{gold_path}/dim_athlete"
        )
    )


    dim_event_df = (
        spark.read
        .format("parquet")
        .load(
            f"{gold_path}/dim_event"
        )
    )


    dim_game_df = (
        spark.read
        .format("parquet")
        .load(
            f"{gold_path}/dim_game"
        )
    )


    # ==========================================
    # Read Silver Data
    # ==========================================

    athlete_events_df = (
        spark.read
        .format("parquet")
        .load(
            f"{silver_path}/athlete_events_clean"
        )
    )


    # ==========================================
    # Create Fact Table
    # ==========================================

    fact_df = (
        athlete_events_df

        .join(
            dim_athlete_df,
            athlete_events_df.ID == dim_athlete_df.ID,
            "left"
        )

        .join(
            dim_event_df,
            [
                athlete_events_df.Sport == dim_event_df.Sport,
                athlete_events_df.Event == dim_event_df.Event,
            ],
            "left"
        )

        .join(
            dim_game_df,
            [
                 athlete_events_df.Year == dim_game_df.Year,
                 athlete_events_df.Season == dim_game_df.Season,
            ],
            "left",
        )
    )


    # ==========================================
    # Select Fact Columns
    # ==========================================

    fact_df = fact_df.select(
        "athlete_key",
        "event_key",
        "game_key",

        "Medal",
        "Team",
        "NOC"
    )


    # ==========================================
    # Add Audit Columns
    # ==========================================

    fact_df = (
        fact_df
        .withColumn(
            "date_insert",
            current_timestamp()
        )
    )


    # ==========================================
    # Save Fact Table
    # ==========================================

    (
        fact_df.write
        .format("parquet")
        .mode("overwrite")
        .save(
            f"{gold_path}/fact_Olympic_Results"
        )
    )