from pyspark.sql.functions import (
    col, count, sha2, concat_ws, lower, regexp_replace, trim
    )

def find_matching_entities(df):
    """
    Find records that share the same normalized email.
    """

    matches = (
        df.groupBy("email")
        .agg(
            count("*").alias("record_count")
        )
        .filter(
            (col("email").isNotNull()) &
            (col("record_count") > 1)
        )
        .orderBy(col("record_count").desc())
    )

    return matches

def add_entity_id(df):

    return df.withColumn(
        "entity_id",
        sha2(
            concat_ws(
                "||",
                col("email")
            ),
            256
        )
    )

def add_company_id(df):
    return df.withColumn(
        "company_id",
        sha2(
            concat_ws(
                "||",
                col("company_normalized")
            ),
            256
        )
    )

def normalize_company(df):

    company = lower(trim(col("company")))

    company = regexp_replace(
        company, 
        r"\b(incorporated|inc|corporation|corp|limited|ltd|llc)\b",
        ""
    )

    company = regexp_replace(
        company,
        r"[^a-z0-9]",
        ""
    )

    return df.withColumn(
        "company_normalized",
        trim(company)
    )