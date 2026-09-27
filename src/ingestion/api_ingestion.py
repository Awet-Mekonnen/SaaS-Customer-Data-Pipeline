import json
from pathlib import Path

from api_client import(
    fetch_crm_customers,
    fetch_marketing_customers,
    fetch_sales_customers
)

# project_root = Path(__file__).resolve().parents[2]

# output_dir = project_root / "data" / "raw" / "api"

def save_api_data():

    output_dir = Path("data/raw/api")

    output_dir.mkdir(
        parents = True,
        exist_ok = True
    )

    crm = fetch_crm_customers()
    marketing = fetch_marketing_customers()
    sales = fetch_sales_customers()

    with open(
        output_dir / "crm.json",
        "w",
        encoding = "utf-8"
    ) as file:
        json.dump(crm, file, indent = 2)


    with open(
        output_dir / "marketing.json",
        "w",
        encoding = "utf-8"
    ) as file:
        json.dump(marketing, file, indent = 2)


    with open(
        output_dir / "sales.json",
        "w",
        encoding = "utf-8"
    ) as file:
        json.dump(sales, file, indent = 2)

    print("API data successfully saved.")


if __name__ == "__main__":
    save_api_data()