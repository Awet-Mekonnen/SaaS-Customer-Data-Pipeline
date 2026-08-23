import json
from pathlib import Path

def fetch_sales_contacts():
    file_path = Path("data/sample/sales_contacts.json")

    with open(file_path, "r") as file:
        data = json.load(file)

    return data

def save_sales_raw(data):
    output_path = Path("data/raw/sales/sales_contacts.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as file:
        json.dump(data, file, indent = 2)