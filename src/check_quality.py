from quality.data_quality import (
    check_missing_values,
    check_duplicate_emails, 
    check_invalid_emails,
    check_empty_entities,
    check_customer_completeness
)

def check_data_quality(df):
    check_missing_values(df)

    check_duplicate_emails(df)

    check_invalid_emails(df)

def check_entity_quality(df):

    check_empty_entities(df)
    
    check_customer_completeness(df)