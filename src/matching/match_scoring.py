from matching.fuzzy_matching import similarity_score

from pyspark.sql.functions import (
    col, when, lit, levenshtein, greatest, length
)

from pyspark.sql.types import (
    DoubleType, StringType
)

def calculate_match_score(df):
    print("Match Scoring Columns")
    print(df.columns)

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
                col("name_distance") /
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
        "name_points",
            when(
            col("name_similarity") >= 90,
                25
            ).when(
                col("name_similarity") >= 75,
                15
            ).when(
                col("name_similarity") >= 60,
                5
            ).otherwise(0)
    )

    df = df.withColumn(
        "company_points",
            when(
                col("company_similarity") >= 90,
                25
            ).when(
                col("company_similarity") >= 75,
                15
            ).when(
                col("company_similarity") >= 60,
                5
            ).otherwise(0)
    )

    df = df.withColumn(
        "match_score",
        col("email_match") * 50
        + col("name_points")
        + col("company_points")
    )

    df = df.withColumn(
        "match_confidence",
        when(
            col("match_score") >= 90,
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
    if score >= 90:
        return "High"
    elif score >= 70:
        return "Medium"

    return "No Match"