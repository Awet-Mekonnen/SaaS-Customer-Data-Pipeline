from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce

def attach_entity_ids(
        customer_data: DataFrame,
        entity_groups: DataFrame
) -> DataFrame:

    customers = customer_data.alias("customers")
    entities = entity_groups.alias("entities")

    resolved = (
        customers
        .join(
            entities,
            col("customers.source_id") == col("entities.source_id"),
            "left"
        )
        .select(
            col("customers.customer_name"),
            col("customers.email"),
            col("customers.company"),
            col("customers.phone"),
            col("customers.source_id"),
            col("customers.source"),
            coalesce(
                col("entities.entity_id"),
                col("customers.entity_id"),
                col("customers.source_id")
            ).alias("entity_id"),
            col("customers.company_normalized")
        )
    )

    return resolved