import json
from pathlib import Path

def fetch_marketing_leads():
    file_path = Path("data/sample/marketing_leads.json")

    with open(file_path, "r") as file:
        data = json.load(file)

    return data

def save_marketing_raw(data):
    output_path = Path("data/raw/marketing/marketing_leads.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as file:
        json.dump(data, file, indent = 2)