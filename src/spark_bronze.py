from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lower, trim, col, greatest, levenshtein, length, when, lit
)
from transformation.silver import (
    transform_crm, 
    transform_marketing, 
    transform_sales
)
from quality.data_quality import (
    check_missing_values,
    check_duplicate_emails, 
    check_invalid_emails
)
from matching.entity_resolution import (
    find_matching_entities,
    add_entity_id,
    add_company_id,
    normalize_company
)

from matching.blocking import create_blocks

from matching.candidate_pairs import generate_candidate_pairs

from matching.match_scoring import calculate_match_score

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("SaaS Customer Data Pipeline")
        .master("local[*]")
        .getOrCreate()
    )

    return spark

def load_bronze_data(spark):
    crm_df = spark.read.option("multiline", "true").json("data/raw/crm/crm_customers.json")

    marketing_df = spark.read.option("multiline", "true").json("data/raw/marketing/marketing_leads.json")

    sales_df = spark.read.option("multiline", "true").json("data/raw/sales/sales_contacts.json")

    return crm_df, marketing_df, sales_df

if __name__ == "__main__":
    spark = create_spark_session()

    crm_df, marketing_df, sales_df = load_bronze_data(spark)

    print("\n===== CRM =====")
    crm_df.printSchema()
    #crm_df.show()

    crm_clean = crm_df.withColumn(
        "email",
        lower(trim(crm_df["email"]))
    )

    crm_clean.show()

    print("\n===== Marketing =====")
    marketing_df.printSchema()
    #marketing_df.show()

    marketing_clean = marketing_df.withColumn(
        "email",
        lower(trim(marketing_df["email"]))
    )

    marketing_clean.show()

    print("\n===== Sales =====")
    sales_df.printSchema()
    #sales_df.show()

    sales_clean = sales_df.withColumn(
        "email",
        lower(trim(sales_df["email"]))
    )

    sales_clean.show()

    print("\n===== Record Counts =====")
    print(f"CRM Records: {crm_df.count()}")
    print(f"Marketing Records: {marketing_df.count()}")
    print(f"Sales Records: {sales_df.count()}")

    crm_silver = transform_crm(crm_df)
    marketing_silver = transform_marketing(marketing_df)
    sales_silver = transform_sales(sales_df)

    silver_df = (
        crm_silver
        .unionByName(marketing_silver)
        .unionByName(sales_silver)
    )

    check_missing_values(silver_df)
    check_duplicate_emails(silver_df)
    check_invalid_emails(silver_df)

    matches = find_matching_entities(silver_df)

    print("\n===== Matching Entities =====")

    matches.show(truncate = False)

    silver_with_entities = add_entity_id(silver_df)

    silver_with_entities = normalize_company(silver_with_entities)

    silver_with_entities = add_company_id(silver_with_entities)

    silver_blocked = create_blocks(silver_with_entities)

    candidates = generate_candidate_pairs(silver_blocked)

    candidates.printSchema()

    scored_candidates = calculate_match_score(candidates)

    high_confidence = scored_candidates.filter(
        col("match_confidence") == "high_confidence"
    )

    medium_confidence = scored_candidates.filter(
        col("match_confidence") == "medium_confidence"
    )

    no_match = scored_candidates.filter(
        col("match_confidence") == "no_match"
    )

    print("\n===== HIGH CONFIDENCE =====")

    high_confidence.select(
        "source_id_1",
        "source_id_2",
        "left_name",
        "right_name",
        "match_score"
    ).show(
        truncate=False
    )

    print("\n===== MEDIUM CONFIDENCE =====")

    medium_confidence.select(
        "source_id_1",
        "source_id_2",
        "left_name",
        "right_name",
        "match_score"
    ).show(
        truncate=False
    )

    # scored_candidates.select(
    #     "source_id_1",
    #     "source_id_2",
    #     "left_name",
    #     "right_name",
    #     "name_points",
    #     "left_company",
    #     "right_company",
    #     "company_points",
    #     "left_email",
    #     "right_email",
    #     "email_match",
    #     "match_score",
    # ).orderBy(
    #     "match_score"
    # ).show(
    #     truncate=False
    # )

    # print("\n===== Candidate Pairs =====")

    # candidates.show(truncate = False)

    # print(f"Candidate pairs :{candidates.count()}")

    # silver_with_entities.select(
    #     "customer_name",
    #     "email",
    #     "company",
    #     "company_normalized",
    #     "source"
    # ).show(truncate = False)

    # silver_blocked.select(
    #     "customer_name",
    #     "email",
    #     "email_domain",
    #     "name_block"
    # ).show(truncate = False)
    # #silver_df.show(truncate = False)
    
    print("Spark Bronze Data Pipeline completed successfully.")
    spark.stop()