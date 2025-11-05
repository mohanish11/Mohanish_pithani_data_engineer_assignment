from utils import get_connection
def create_taxes_table(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS home_db.taxes (
    property_id INT PRIMARY KEY,
    Taxes DECIMAL(10,2)
     )""")
def insert_into_taxes(cur):
    cur.execute("""
        INSERT INTO home_db.taxes (property_id, Taxes)
        SELECT
            raw_id AS property_id,
            CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Taxes')) AS DECIMAL(10,2)) AS Taxes
        FROM home_db.raw_property rp
        WHERE NOT EXISTS (
            SELECT 1 FROM home_db.taxes t WHERE t.property_id = rp.raw_id
        );
    """)
def main():
    conn = get_connection()
    cur = conn.cursor()
    create_taxes_table(cur)
    insert_into_taxes(cur)
    conn.commit()
    print("✅ Inserted data into taxes table")
    cur.close()
    conn.close()
if __name__ == "__main__":
    main()

               
