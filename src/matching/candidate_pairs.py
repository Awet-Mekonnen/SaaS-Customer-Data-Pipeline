from pyspark.sql.functions import (
    col, least, greatest
)

def generate_candidate_pairs(df):

    left = df.alias("left")
    right = df.alias("right")

    candidate = (
        left.join(
            right,
            (
                (col("left.email_domain") == col("right.email_domain")) |
                (col("left.name_block") == col("right.name_block"))
            )
            &
            (col("left.source_id") != col("right.source_id"))
        ).select(
            least(col("left.source_id"), col("right.source_id")).
            alias("source_id_1"),
            greatest(col("left.source_id"), col("right.source_id")).
            alias("source_id_2"),

            col("left.customer_name").alias("left_name"),
            col("right.customer_name").alias("right_name"),

            col("left.email").alias("left_email"),
            col("right.email").alias("right_email"),

            col("left.phone").alias("left_phone"),
            col("right.phone").alias("right_phone"),

            col("left.company_normalized").alias("left_company"),
            col("right.company_normalized").alias("right_company"),

            col("left.source").alias("left_source"),
            col("right.source").alias("right_source")
        )
        .dropDuplicates(
            ["source_id_1", "source_id_2"]
            )
    )

    return candidate