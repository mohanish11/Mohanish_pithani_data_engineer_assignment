#!/usr/bin/env python3
import json
import os
from utils import get_connection

JSON_PATH = os.path.join(os.path.dirname(__file__), "../data/fake_property_data_new.json")

def ensure_staging_table(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS raw_property (
        raw_id INT AUTO_INCREMENT PRIMARY KEY,
        raw_json LONGTEXT NOT NULL,
        ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_file VARCHAR(255)
    );
    """)

def main():
    conn = get_connection()
    cur = conn.cursor()
    ensure_staging_table(cur)

    with open(JSON_PATH, "r") as f:
        text = f.read().strip()
    data = json.loads(text) if text.startswith("[") else [json.loads(line) for line in text.splitlines()]

    cur.executemany(
        "INSERT INTO raw_property (raw_json, source_file) VALUES (%s, %s)",
        [(json.dumps(d), os.path.basename(JSON_PATH)) for d in data]
    )
    conn.commit()
    print(f"✅ Loaded {len(data)} records into raw_property")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
