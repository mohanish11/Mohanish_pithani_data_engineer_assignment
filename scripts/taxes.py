from utils import get_connection
def create_taxes_table(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS home_db.taxes (
    property_id INT PRIMARY KEY,
    Taxes DECIMAL(10,2),
    FOREIGN KEY (property_id)
        REFERENCES home_db.raw_property(raw_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
     )""")
def insert_into_taxes(cur):
    cur.execute("""
        INSERT INTO home_db.taxes (property_id, Taxes)
        SELECT
            raw_id AS property_id,
            CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Taxes')) AS DECIMAL(10,2)) AS Taxes
        FROM home_db.raw_property;
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

               
