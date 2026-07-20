from pyspark.sql import SparkSession

from pipeline import run_pipeline


def create_spark_session() -> SparkSession:
    """
    Create Spark session.
    """

    return (
        SparkSession.builder
        .appName("IOC")
        .getOrCreate()
    )


def main() -> None:

    spark = create_spark_session()

    try:
        run_pipeline(spark)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()