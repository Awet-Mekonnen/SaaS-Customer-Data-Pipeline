from pyspark.sql.functions import col, concat_ws, lower, trim, lit
from pyspark.sql.types import StringType


def transform_crm(crm_df):

    crm_silver = crm_df.select(
        concat_ws(
            " ",
            trim(col("first_name")),
            trim(col("last_name"))
        ).alias("customer_name"),

        lower(trim(col("email"))).alias("email"),

        trim(col("company")).alias("company"),

        trim(col("phone")).alias("phone"),

        col("customer_id").alias("source_id"),

        lit("crm").alias("source")
    )

    return crm_silver


def transform_marketing(marketing_df):

    marketing_silver = marketing_df.select(
        trim(col("name")).alias("customer_name"),

        lower(trim(col("email"))).alias("email"),

        trim(col("company_name")).alias("company"),

        lit(None).cast(StringType()).alias("phone"),

        col("lead_id").alias("source_id"),

        lit("marketing").alias("source")
    )

    return marketing_silver


def transform_sales(sales_df):

    sales_silver = sales_df.select(
        trim(col("contact_name")).alias("customer_name"),

        lower(trim(col("email"))).alias("email"),

        trim(col("organization")).alias("company"),

        lit(None).cast(StringType()).alias("phone"),

        col("contact_id").alias("source_id"),

        lit("sales").alias("source")
    )

    return sales_silver