from ingestion.crm import fetch_crm_customers, save_crm_raw
from ingestion.marketing import fetch_marketing_leads, save_marketing_raw
from ingestion.sales import fetch_sales_contacts, save_sales_raw

def main():
    crm_data = fetch_crm_customers()
    marketing_data = fetch_marketing_leads()
    sales_data = fetch_sales_contacts()

    save_crm_raw(crm_data)
    save_marketing_raw(marketing_data)
    save_sales_raw(sales_data)

    print("Raw CRM data saved successfully.")
    print("Raw Marketing data saved successfully.")
    print("Raw Sales data saved successfully.")

if __name__ == "__main__":
    main()