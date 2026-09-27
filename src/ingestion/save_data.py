from datetime import datetime, timezone
import os
import json

def save_data(df, source, base_path):
    """
    Save a DataFrame as a timestamped Bronze snapshot.
    """
    source_path = os.path.join(
        base_path,
        source
    )

    if not os.path.exists(source_path): 
        os.makedirs(source_path) 
        print(f"Created Bronze directory: {source_path}")

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")


    output_path = os.path.join(
        source_path,
        f"{source}_{timestamp}.json"
    )

    records = [row.asDict() for row in df.collect()]

    with open(output_path, "w", encoding = "utf-8") as file:
        json.dump(records, file, indent = 4)

    print(
        f"\nSaved: {source}"
        f"\nOutput path: {base_path}{source}"
        f"\nTimestamp: {timestamp}"
    )

    return output_path