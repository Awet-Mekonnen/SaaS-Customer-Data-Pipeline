from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, concat, least, greatest, sha2, min as spark_min, substr
)


def build_entity_groups(high_confidence: DataFrame, customer_data: DataFrame) -> DataFrame:
    """
    Build entity groups from high-confidence match pairs.

    Each connected group of source IDs receives a shared entity_id.
    """
    all_ids = customer_data.select(
        "source_id"
    ).distinct()


    pairs = high_confidence.select(
        least(
            col("source_id_1"),
            col("source_id_2")
        ).alias("id_1"),

        greatest(
            col("source_id_1"),
            col("source_id_2")
        ).alias("id_2")
    ).distinct()


    entity_groups = all_ids.withColumn(
        "entity_root",
        col("source_id")
    )

    for _ in range(3):

        left_matches = (
            pairs
            .join(
                entity_groups.alias("e"),
                col("id_1") == col("e.source_id"),
                "left"
            )
            .select(
                col("id_2").alias("source_id"),
                col("e.entity_root").alias("candidate_root")
            )
        )

        right_matches = (
            pairs
            .join(
                entity_groups.alias("e"),
                col("id_2") == col("e.source_id"),
                "left"
            )
            .select(
                col("id_1").alias("source_id"),
                col("e.entity_root").alias("candidate_root")
            )
        )

        connections = (
            left_matches
            .unionByName(right_matches)
            .filter(col("candidate_root").isNotNull())
        )

        smallest_root = (
            connections
            .groupBy("source_id")
            .agg(
                spark_min("candidate_root")
                .alias("candidate_root")
            )
        )

        entity_groups = (
            entity_groups
            .join(
                smallest_root,
                "source_id",
                "left"
            )
            .withColumn(
                "entity_root",
                least(
                    col("entity_root"),
                    col("candidate_root")
                )
            )
            .drop("candidate_root")
        )

        #----------------------------------
        # Convert temporary source ids into
        # canonical ENT-xxxx ids
        #----------------------------------

    entity_groups = entity_groups.withColumn(
        "entity_id",
        concat(
            lit("ENT-"),
            sha2(
                col("entity_root"),
                256
            ).substr(1, 12)
        )
    )

    # ---------------------------------------
    # 8. Return only the required columns
    # ---------------------------------------

    return entity_groups.select(
        "source_id",
        "entity_id"
    )