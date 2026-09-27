# from matching.fuzzy_matching import similarity_score

from pyspark.sql.functions import (
    col, when, lit, levenshtein, greatest, length, regexp_replace
)

from pyspark.sql.types import (
    DoubleType, StringType
)

def calculate_match_score(df):

    df = df.withColumn(
        "name_distance",
        levenshtein(
            col("left_name"),
            col("right_name")
        )
    )

    df = df.withColumn(
        "name_similarity",
        (
            1 - 
            (
                col("name_distance") /
                greatest(
                    length(col("left_name")),
                    length(col("right_name"))
                )
            )
        ) * 100
    )

    df = df.withColumn(
        "company_distance",
        levenshtein(
            col("left_company"),
            col("right_company")
        )
    )

    df = df.withColumn(
        "company_similarity",
        (
            1 - 
            (
                col("company_distance") /
                greatest(
                    length(col("left_company")),
                    length(col("right_company"))
                )
            )
        ) * 100    
    )

    df = df.withColumn(
        "email_match",
        when(
            (col("left_email").isNotNull()) &
            (col("right_email").isNotNull()) &
            (col("left_email") == col("right_email")),
            1
        ).otherwise(0)
    )

    df = df.withColumn(
        "left_phone_normalized",
        regexp_replace(col("left_phone"), r"[^0-9]", "")
    )

    df = df.withColumn(
        "right_phone_normalized",
        regexp_replace(col("right_phone"), r"[^0-9]", "")
    )

    df = df.withColumn(
        "phone_match",
        when(
            (col("left_phone_normalized").isNotNull()) &
            (col("right_phone_normalized").isNotNull()) &
            (col("left_phone_normalized") == col("right_phone_normalized")),
            1
        ).otherwise(0)
    )

    df = df.withColumn(
        "name_points",
            when(
            col("name_similarity") >= 90,
                25
            ).when(
                col("name_similarity") >= 65,
                15
            ).when(
                col("name_similarity") >= 40,
                5
            ).otherwise(0)
    )

    df = df.withColumn(
        "company_points",
            when(
                col("company_similarity") >= 90,
                25
            ).when(
                col("company_similarity") >= 65,
                15
            ).when(
                col("company_similarity") >= 40,
                5
            ).otherwise(0)
    )

    df = df.withColumn(
        "match_score",
        col("email_match") * 25
        + col("phone_match") * 25
        + col("name_points")
        + col("company_points")
    )

    df = df.withColumn(
        "match_confidence",
        when(
            col("match_score") >= 80,
            "high_confidence"
        ).when(
            col("match_score") >= 50,
            "medium_confidence"
        ).otherwise(
            "no_match"
        )
    )
    return df

def classify_match(score):
    if score >= 80:
        return "High"
    elif score >= 50:
        return "Medium"

    return "No Match"