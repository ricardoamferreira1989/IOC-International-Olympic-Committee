from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number,
    current_timestamp,
    lit,
    col,
    monotonically_increasing_id,
)
from pyspark.sql.utils import AnalysisException


# ==========================================
# Gold Dimension Layer
# ==========================================

def create_dimensions(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
) -> None:
    """
    Create Gold dimension tables from Silver layer.
    """

    # ==========================================
    # Read Silver Layer Data
    # ==========================================

    athlete_df = (
        spark.read
        .format("parquet")
        .load(
            f"{silver_path}/athlete_clean"
        )
    )


    event_df = (
        spark.read
        .format("parquet")
        .load(
            f"{silver_path}/event_clean"
        )
    )


    game_df = (
        spark.read
        .format("parquet")
        .load(
            f"{silver_path}/game_clean"
        )
    )


    load_timestamp = current_timestamp()


    # ==========================================
    # Dim Athlete
    # SCD Type 2
    # Tracks Weight and Height changes
    # ==========================================

    try:

        existing_dim = (
            spark.read
            .format("parquet")
            .load(
                f"{gold_path}/dim_athlete"
            )
        )

    except AnalysisException:

        existing_dim = None



    # ------------------------------------------
    # Initial Load
    # ------------------------------------------

    if existing_dim is None:


        dim_athlete_df = (
            athlete_df

            .withColumn(
                "athlete_key",
                row_number()
                .over(
                    Window.orderBy("ID")
                )
            )

            .withColumn(
                "valid_from",
                load_timestamp
            )

            .withColumn(
                "valid_to",
                lit(None).cast("timestamp")
            )

            .withColumn(
                "current_flag",
                lit(True)
            )

            .withColumn(
                "date_insert",
                load_timestamp
            )
        )


    # ------------------------------------------
    # Incremental SCD Type 2 Load
    # ------------------------------------------

    else:


        current_records = (
            existing_dim
            .filter(
                col("current_flag") == True
            )
        )


        # Detect changes

        changed_records = (

            athlete_df.alias("new")

            .join(
                current_records.alias("old"),
                "ID",
                "inner"
            )

            .filter(

                (col("new.Weight") != col("old.Weight"))
                |
                (col("new.Height") != col("old.Height"))
                |
                (col("new.Team") != col("old.Team"))
                |
                (col("new.NOC") != col("old.NOC"))

            )
        )


        changed_ids = (
            changed_records
            .select("ID")
            .distinct()
        )


        # Close previous versions

        expired_records = (

            current_records

            .join(
                changed_ids,
                "ID",
                "inner"
            )

            .withColumn(
                "valid_to",
                load_timestamp
            )

            .withColumn(
                "current_flag",
                lit(False)
            )
        )


        # Create new versions

        new_versions = (

            changed_records

            .select(
                col("new.ID"),
                col("new.Name"),
                col("new.Sex"),
                col("new.Age"),
                col("new.Height"),
                col("new.Weight"),
                col("new.Team"),
                col("new.NOC")
            )

            .withColumn(
                "athlete_key",
                monotonically_increasing_id()
            )

            .withColumn(
                "valid_from",
                load_timestamp
            )

            .withColumn(
                "valid_to",
                lit(None).cast("timestamp")
            )

            .withColumn(
                "current_flag",
                lit(True)
            )

            .withColumn(
                "date_insert",
                load_timestamp
            )
        )


        unchanged_records = (

            current_records

            .join(
                changed_ids,
                "ID",
                "left_anti"
            )

        )


        dim_athlete_df = (

            unchanged_records

            .unionByName(
                expired_records
            )

            .unionByName(
                new_versions
            )

        )


    (
        dim_athlete_df.write
        .format("parquet")
        .mode("overwrite")
        .save(
            f"{gold_path}/dim_athlete"
        )
    )


    # ==========================================
    # Dim Event
    # SCD Type 1
    # ==========================================

    dim_event_df = (

        event_df

        .withColumn(
            "event_key",
            row_number()
            .over(
                Window.orderBy("Event")
            )
        )

        .withColumn(
            "date_insert",
            load_timestamp
        )
    )


    dim_event_df = dim_event_df.select(
        "event_key",
        "Event",
        "Sport",
        "date_insert",
    )


    (
        dim_event_df.write
        .format("parquet")
        .mode("overwrite")
        .save(
            f"{gold_path}/dim_event"
        )
    )



    # ==========================================
    # Dim Game
    # SCD Type 0
    # Immutable Dimension
    # ==========================================


    try:

        existing_games = (
            spark.read
            .format("parquet")
            .load(
                f"{gold_path}/dim_game"
            )
        )


        # Only insert new Olympic Games
        new_games = (

            game_df

            .join(
                existing_games,
                "Year",
                "left_anti"
            )

        )


        new_games = (

            new_games

            .withColumn(
                "game_key",
                row_number()
                .over(
                    Window.orderBy("Year")
                )
            )

            .withColumn(
                "date_insert",
                load_timestamp
            )

        )


        final_games = (

            existing_games

            .unionByName(
                new_games
            )

        )


    except Exception:


        # First load

        final_games = (

            game_df

            .withColumn(
                "game_key",
                row_number()
                .over(
                    Window.orderBy("Year")
                )
            )

            .withColumn(
                "date_insert",
                load_timestamp
            )

        )



    final_games = final_games.select(
        "game_key",
        "Year",
        "Season",
        "City",
        "date_insert",
    )


    (
        final_games.write
        .format("parquet")
        .mode("overwrite")
        .save(
            f"{gold_path}/dim_game"
        )
    )