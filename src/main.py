from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lower, trim, col, greatest, 
    levenshtein, length, when, lit
)
from transform_data import transform_data
from check_quality import (
    check_data_quality, check_entity_quality 
)
from import_files import load_data
from matching_modules import matching
from transformation.gold import build_customer_360
from ingestion.save_data import save_data


def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("SaaS Customer Data Pipeline")
        .master("local[*]")
        .getOrCreate()
    )

    return spark


if __name__ == "__main__":

    spark = create_spark_session()

    crm_df, marketing_df, sales_df = load_data(spark)

    silver_df = transform_data(crm_df, marketing_df, sales_df)

    silver_path = save_data(silver_df, "silver", "reports/")

    check_data_quality(silver_df)

    resolved_customer = matching(silver_df)

    customer_360 = build_customer_360(
        resolved_customer
    )

    check_entity_quality(
        customer_360
    )

    customer_360_path = save_data(
        customer_360, "customer_360", "reports/"
    )

    print("Spark Bronze Data Pipeline completed successfully.")
    spark.stop()