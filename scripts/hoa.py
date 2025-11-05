from utils import get_connection
def create_hoa_table(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS home_db.hoa (
    HOA INT,
    HOA_Flag VARCHAR(10),
    property_id INT,
    FOREIGN KEY (property_id)
        REFERENCES home_db.raw_property(raw_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
    );""")
def insert_into_hoa(cur):
    cur.execute("""
    INSERT INTO home_db.hoa (HOA, HOA_Flag,property_id)SELECT
    JSON_UNQUOTE(JSON_EXTRACT(hoaitem.value, '$.HOA'))       AS HOA,
    JSON_UNQUOTE(JSON_EXTRACT(hoaitem.value, '$.HOA_Flag'))  AS HOA_Flag,
    raw_id AS property_id
    FROM home_db.raw_property,
     JSON_TABLE(raw_json, '$.HOA[*]'
        COLUMNS (
            value JSON PATH '$'
        )
     ) AS hoaitem;
       """)
def main():
    conn = get_connection()
    cur = conn.cursor()
    create_hoa_table(cur)
    insert_into_hoa(cur)
    conn.commit()
    print("✅ Inserted data into HOA table")
    cur.close()
    conn.close()
if __name__ == "__main__":
    main()

               
