from utils import get_connection
def create_hoa_table(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS home_db.hoa (
    HOA INT,
    HOA_Flag VARCHAR(10), property_id INT
    );""")
def insert_into_hoa(cur):
    cur.execute("""
        INSERT INTO home_db.hoa (HOA, HOA_Flag, property_id)
    SELECT
        source_data.HOA,
        source_data.HOA_Flag,
        source_data.property_id
    FROM (
        -- This is your original query that unnests the JSON
        SELECT
            JSON_UNQUOTE(JSON_EXTRACT(hoaitem.value, '$.HOA'))      AS HOA,
            JSON_UNQUOTE(JSON_EXTRACT(hoaitem.value, '$.HOA_Flag')) AS HOA_Flag,
            raw.raw_id AS property_id
        FROM 
            home_db.raw_property AS raw,
            JSON_TABLE(raw.raw_json, '$.HOA[*]'
                COLUMNS (
                    value JSON PATH '$'
                )
            ) AS hoaitem
    ) AS source_data
    WHERE NOT EXISTS (
        -- This checks if a matching row already exists in the target table
        SELECT 1
        FROM home_db.hoa AS target
        WHERE
            target.HOA = source_data.HOA
            AND target.HOA_Flag = source_data.HOA_Flag
            AND target.property_id = source_data.property_id
    );
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

               
