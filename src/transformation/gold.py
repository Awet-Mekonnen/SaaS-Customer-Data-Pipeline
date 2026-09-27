from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    first, collect_set, concat_ws, sort_array, count
)

def build_customer_360(resolved_customers: DataFrame) -> DataFrame:

    customer_360 = (
        resolved_customers
        .groupBy("entity_id")
        .agg(
            first("customer_name", ignorenulls = True)
            .alias(
                "customer_name"
            ),

            first("email", ignorenulls = True)
            .alias(
                "email"
            ),

            first("company", ignorenulls = True)
            .alias(
                "company"
            ),

            first("phone", ignorenulls = True)
            .alias(
                "phone"
            ),
            count("*").alias("source_count"),

            concat_ws(
                ", ",
                sort_array(
                    collect_set("source")
                )
            ).alias("sources"),

            concat_ws(
                ", ",
                sort_array(
                    collect_set("source_id")
                )
            ).alias("source_ids")
        )
        .orderBy("customer_name")
    )

    return customer_360