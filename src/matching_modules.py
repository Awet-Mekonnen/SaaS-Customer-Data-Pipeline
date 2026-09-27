from pyspark.sql.functions import col

from matching.entity_resolution import (
    find_matching_entities,
    add_entity_id,
    add_company_id,
    normalize_company
)

from matching.blocking import create_blocks

from matching.candidate_pairs import generate_candidate_pairs

from matching.match_scoring import calculate_match_score

from matching.entity_groups import build_entity_groups

from matching.entity_manager import attach_entity_ids

from ingestion.save_data import save_data

def matching(df):
    
    matches = find_matching_entities(df)
    
    silver_with_entities = add_entity_id(df)
    
    silver_with_entities = normalize_company(silver_with_entities)
    
    silver_with_entities = add_company_id(silver_with_entities)

    silver_blocked = create_blocks(silver_with_entities)

    candidates = generate_candidate_pairs(silver_blocked)

    scored_candidates = calculate_match_score(candidates)

    high_confidence = scored_candidates.filter(
        col("match_confidence") == "high_confidence"
    )

    high_confidence_path = high_confidence.select(
        "left_name",
        "right_name",
        "email_match",
        "phone_match",
        "company_similarity",
        "match_score"
    ).orderBy(
        col("match_score").asc()
    )

    high_confidence_path = save_data(
        high_confidence_path, "high_confidence", "reports/high_confidence"
    )

    trusted_matches = scored_candidates.filter(
        (col("match_score") >= 80) |
        (
            (col("match_score") >= 50) &
            (col("email_match") == 1)
        ) |
        (
            (col("email_match") == 1) &
            (col("phone_match") == 1)
        )
    )

    entity_groups = build_entity_groups(
        trusted_matches, silver_with_entities
    )

    resolved_customer = attach_entity_ids(
        silver_with_entities,
        entity_groups
    )

    resolved_customer_path = save_data(resolved_customer, "resolved_customer", "reports/resolved_customer")

    return resolved_customer
