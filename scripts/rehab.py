from utils import get_connection
def create_rehab_table(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS home_db.rehab (
    rehab_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT,
    Underwriting_Rehab DECIMAL(10,2),
    Rehab_Calculation DECIMAL(10,2),
    Paint VARCHAR(10),
    Flooring_Flag VARCHAR(10),
    Foundation_Flag VARCHAR(10),
    Roof_Flag VARCHAR(10),
    HVAC_Flag VARCHAR(10),
    Kitchen_Flag VARCHAR(10),
    Bathroom_Flag VARCHAR(10),
    Appliances_Flag VARCHAR(10),
    Windows_Flag VARCHAR(10),
    Landscaping_Flag VARCHAR(10),
    Trashout_Flag VARCHAR(10));
    """)
def insert_into_rehab(cur):
    cur.execute("TRUNCATE TABLE home_db.rehab;")
    cur.execute("""
               INSERT INTO home_db.rehab (
    property_id,
    Underwriting_Rehab,
    Rehab_Calculation,
    Paint,
    Flooring_Flag,
    Foundation_Flag,
    Roof_Flag,
    HVAC_Flag,
    Kitchen_Flag,
    Bathroom_Flag,
    Appliances_Flag,
    Windows_Flag,
    Landscaping_Flag,
    Trashout_Flag
  )
    SELECT
    r.raw_id AS property_id,
    JSON_UNQUOTE(JSON_EXTRACT(rehabitem.value, '$.Underwriting_Rehab')),
    JSON_UNQUOTE(JSON_EXTRACT(rehabitem.value, '$.Rehab_Calculation')),
    JSON_UNQUOTE(JSON_EXTRACT(rehabitem.value, '$.Paint')),
    JSON_UNQUOTE(JSON_EXTRACT(rehabitem.value, '$.Flooring_Flag')),
    JSON_UNQUOTE(JSON_EXTRACT(rehabitem.value, '$.Foundation_Flag')),
    JSON_UNQUOTE(JSON_EXTRACT(rehabitem.value, '$.Roof_Flag')),
    JSON_UNQUOTE(JSON_EXTRACT(rehabitem.value, '$.HVAC_Flag')),
    JSON_UNQUOTE(JSON_EXTRACT(rehabitem.value, '$.Kitchen_Flag')),
    JSON_UNQUOTE(JSON_EXTRACT(rehabitem.value, '$.Bathroom_Flag')),
    JSON_UNQUOTE(JSON_EXTRACT(rehabitem.value, '$.Appliances_Flag')),
    JSON_UNQUOTE(JSON_EXTRACT(rehabitem.value, '$.Windows_Flag')),
    JSON_UNQUOTE(JSON_EXTRACT(rehabitem.value, '$.Landscaping_Flag')),
    JSON_UNQUOTE(JSON_EXTRACT(rehabitem.value, '$.Trashout_Flag'))
    FROM home_db.raw_property AS r,
     JSON_TABLE(r.raw_json, '$.Rehab[*]'
        COLUMNS (
            value JSON PATH '$'
        )
     ) AS rehabitem;

""")

def main():
    conn = get_connection()
    cur = conn.cursor()
    create_rehab_table(cur)
    insert_into_rehab(cur)
    conn.commit()
    print("✅ Inserted data into rehab table")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
