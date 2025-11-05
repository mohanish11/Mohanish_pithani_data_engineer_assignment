from utils import get_connection
def create_valuation_table(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS home_db.valuation (
    valuation_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT,
    Previous_Rent DECIMAL(10,2),
    List_Price DECIMAL(10,2),
    Zestimate DECIMAL(10,2),
    ARV DECIMAL(10,2),
    Expected_Rent DECIMAL(10,2),
    Rent_Zestimate DECIMAL(10,2),
    Low_FMR DECIMAL(10,2),
    High_FMR DECIMAL(10,2),
    Redfin_Value DECIMAL(10,2),
    FOREIGN KEY (property_id)
        REFERENCES home_db.raw_property(raw_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
    )""")
def insert_into_valuation(cur):
    cur.execute("""
    INSERT INTO home_db.valuation (
    property_id,
    Previous_Rent,
    List_Price,
    Zestimate,
    ARV,
    Expected_Rent,
    Rent_Zestimate,
    Low_FMR,
    High_FMR,
    Redfin_Value
)
SELECT
    r.raw_id AS property_id,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(valitem.value, '$.Previous_Rent')) AS DECIMAL(10,2)) AS Previous_Rent,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(valitem.value, '$.List_Price')) AS DECIMAL(10,2))    AS List_Price,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(valitem.value, '$.Zestimate')) AS DECIMAL(10,2))      AS Zestimate,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(valitem.value, '$.ARV')) AS DECIMAL(10,2))            AS ARV,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(valitem.value, '$.Expected_Rent')) AS DECIMAL(10,2))  AS Expected_Rent,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(valitem.value, '$.Rent_Zestimate')) AS DECIMAL(10,2)) AS Rent_Zestimate,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(valitem.value, '$.Low_FMR')) AS DECIMAL(10,2))        AS Low_FMR,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(valitem.value, '$.High_FMR')) AS DECIMAL(10,2))       AS High_FMR,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(valitem.value, '$.Redfin_Value')) AS DECIMAL(10,2))   AS Redfin_Value
    FROM home_db.raw_property AS r,
     JSON_TABLE(r.raw_json, '$.Valuation[*]'
        COLUMNS (
            value JSON PATH '$'
        )
     ) AS valitem;
     """)
def main():
    conn = get_connection()
    cur = conn.cursor()
    create_valuation_table(cur)
    insert_into_valuation(cur)
    conn.commit()
    print("✅ Inserted data into valuation table")
    cur.close()
    conn.close()
if __name__ == "__main__":
    main()