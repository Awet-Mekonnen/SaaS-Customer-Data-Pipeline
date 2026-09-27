from pathlib import Path

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    lower, trim, col, greatest,
)

from ingestion.save_data import save_data

PROJECT_ROOT = Path(__file__).resolve().parents[1]

API_DATA_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "api"
)

CRM_API_PATH = API_DATA_DIR / "crm.json"
MARKETING_API_PATH = API_DATA_DIR / "marketing.json"
SALES_API_PATH = API_DATA_DIR / "sales.json"

def load_data(spark):

    crm_df = spark.read.option(
        "multiLine",
        True
    ).json(
        str(CRM_API_PATH)
    )

    marketing_df = spark.read.option(
        "multiLine",
        True
    ).json(
        str(MARKETING_API_PATH)
    )

    sales_df = spark.read.option(
        "multiLine",
        True

    ).json(
        str(SALES_API_PATH)
    )

    crm_bronze_path = save_data(crm_df, "crm", "reports/bronze")
    marketing_bronze_path = save_data(marketing_df, "marketing", "reports/bronze")
    sales_bronze_path = save_data(sales_df, "sales", "reports/bronze")
    
    crm_df = spark.read.option("multiline", True).json(crm_bronze_path)
    marketing_df = spark.read.option("multiline", True).json(marketing_bronze_path)
    sales_df = spark.read.option("multiline", True).json(sales_bronze_path)

    crm_clean = crm_df.withColumn(
        "email",
        lower(trim(crm_df["email"]))
    )

    marketing_clean = marketing_df.withColumn(
        "email",
        lower(trim(marketing_df["email"]))
    )

    sales_clean = sales_df.withColumn(
        "email",
        lower(trim(sales_df["email"]))
    )

    print("\n===== Record Counts =====")
    print(f"CRM Records: {crm_df.count()}")
    print(f"Marketing Records: {marketing_df.count()}")
    print(f"Sales Records: {sales_df.count()}")    

    return crm_df, marketing_df, sales_df