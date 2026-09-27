from transformation.silver import (
    transform_crm, 
    transform_marketing, 
    transform_sales
)

def transform_data(crm_df, marketing_df, sales_df):
    crm_silver = transform_crm(crm_df)
    marketing_silver = transform_marketing(marketing_df)
    sales_silver = transform_sales(sales_df)

    silver_df = (
        crm_silver
        .unionByName(marketing_silver)
        .unionByName(sales_silver)
    )

    return silver_df