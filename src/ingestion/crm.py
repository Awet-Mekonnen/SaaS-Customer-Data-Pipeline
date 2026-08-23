import json
from pathlib import Path

def fetch_crm_customers():
    file_path = Path("data/sample/crm_customers.json")

    with open(file_path, "r") as file:
        data = json.load(file)

    return data

def save_crm_raw(data):
    output_path = Path("data/raw/crm/crm_customers.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as file:
        json.dump(data, file, indent = 2)