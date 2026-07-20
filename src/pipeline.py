from pyspark.sql import SparkSession

from bronze.ingest import ingest_bronze
from silver.transformation import transform_silver
from gold.dimensions import create_dimensions
from gold.fact import create_fact


def run_pipeline(
    spark: SparkSession,
) -> None:
    """
    Execute IOC Olympic data pipeline.
    """
    # ==========================================
    # Bronze Layer
    # ==========================================

    ingest_bronze(
        spark,
        input_path="/data/source/athlete_events.csv",
        output_path="/data/bronze/stg_athlete_events",
    )

    # ==========================================
    # Silver Layer
    # ==========================================

    transform_silver(
        spark,
        bronze_path="/data/bronze/stg_athlete_events",
        silver_path="/data/silver",
    )


    # ==========================================
    # Gold Dimension Layer
    # ==========================================

    create_dimensions(
        spark,
        silver_path="/data/silver",
        gold_path="/data/gold",
    )


    # ==========================================
    # Gold Fact Layer
    # ==========================================

    create_fact(
        spark,
        silver_path="/data/silver",
        gold_path="/data/gold",
    )