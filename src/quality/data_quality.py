from pyspark.sql.functions import col, column, count, when

def check_missing_values(df):
    print("\n===== Missing Values =====")

    df.select(
        [
            count(
                when(col(column).isNull() | (col(column) == ""), column)
            ).alias(column)
            for column in df.columns
        ]
    ).show()

def check_duplicate_emails(df):
    print("\n===== Duplicate Emails =====")

    (
        df.groupBy("email")
        .count()
        .filter(col("count") > 1)
        .orderBy(col("count").desc())
        .show(truncate = False)
    )

def check_invalid_emails(df):
    print("\n===== Invalid Emails =====")

    (
        df.filter(
            ~col("email").rlike(
                r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
            )
        )
        .select("customer_name", "email")
        .show(truncate = False)
    )