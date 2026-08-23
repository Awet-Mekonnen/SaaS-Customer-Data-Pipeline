from pyspark.sql.functions import (
    col, lower, regexp_extract, substring
)

def create_blocks(df):

    return ( df.withColumn(
            "email_domain",
            lower(
                regexp_extract(
                    col("email"),
                    r"@(.+)$",
                    1
                )
            )
        ).withColumn(
            "name_block",
            lower(
                substring(
                    col("customer_name"),
                    1,
                    1
                )
            )
        )
    )