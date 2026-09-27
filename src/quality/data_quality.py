from pyspark.sql.functions import (
    col, count, when, aggregate, grouping, trim
)
def check_missing_values(df):

    df.select(
        [
            count(
                when(col(column).isNull() | (col(column) == ""), column)
            ).alias(column)
            for column in df.columns
        ]
    )

def check_duplicate_emails(df):

    (
        df.groupBy("email")
        .count()
        .filter(col("count") > 1)
        .orderBy(col("count").desc())
    )

def check_invalid_emails(df):

    (
        df.filter(
            ~col("email").rlike(
                r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
            )
        )
        .select("customer_name", "email")
    )

def check_empty_entities(df):

    (
        df.filter(
            col("entity_id").isNull() | (trim(col("entity_id")) == "")
        )
        .select(
            "entity_id",
            "customer_name",
            "email"
        )
    )

def check_customer_completeness(df):

    total = df.count()

    if total == 0:
        print("No customer records found.")
        return

    missing_email = df.filter(
        col("email").isNull()
        | (trim(col("email")) == "")
    ).count()

    missing_company = df.filter(
        col("company").isNull()
        | (trim(col("company")) == "")
    ).count()

    missing_phone = df.filter(
        col("phone").isNull()
        | (trim(col("phone")) == "")
    ).count()

    print(f"Total customers: {total}")
    print(
        f"Missing email: {missing_email} "
        f"({missing_email / total * 100:.2f}%)"
    )
    print(
        f"Missing company: {missing_company} "
        f"({missing_company / total * 100:.2f}%)"
    )
    print(
        f"Missing phone: {missing_phone} "
        f"({missing_phone / total * 100:.2f}%)"
    )