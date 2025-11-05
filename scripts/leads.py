from utils import get_connection
def create_leads_table(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS home_db.leads (
        property_id            INT PRIMARY KEY,
        Reviewed_Status        LONGTEXT,
        Most_Recent_Status     LONGTEXT,
        Source                 LONGTEXT,
        Occupancy              LONGTEXT,
        Net_Yield              DECIMAL(10,2),
        IRR                    DECIMAL(10,2),
        Selling_Reason         LONGTEXT,
        Seller_Retained_Broker LONGTEXT,
        Final_Reviewer         LONGTEXT
    );""")
def insert_into_leads(cur):
    cur.execute("""
           INSERT INTO home_db.leads (
        property_id,
        Reviewed_Status,
        Most_Recent_Status,
        Source,
        Occupancy,
        Net_Yield,
        IRR,
        Selling_Reason,
        Seller_Retained_Broker,
        Final_Reviewer
    )
    SELECT
        raw_id as property_id,
        JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Reviewed_Status'))       AS Reviewed_Status,
        JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Most_Recent_Status'))    AS Most_Recent_Status,
        JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Source'))                AS Source,
        JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Occupancy'))             AS Occupancy,
        CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Net_Yield')) AS DECIMAL(10,2))   AS Net_Yield,
        CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.IRR')) AS DECIMAL(10,2))         AS IRR,
        JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Selling_Reason'))        AS Selling_Reason,
        JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Seller_Retained_Broker')) AS Seller_Retained_Broker,
        JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Final_Reviewer'))        AS Final_Reviewer
    FROM home_db.raw_property rp
    WHERE NOT EXISTS (
        SELECT 1 FROM home_db.leads l WHERE l.property_id = rp.raw_id
    );
    """)
def main():
    conn = get_connection()
    cur = conn.cursor()
    create_leads_table(cur)
    insert_into_leads(cur)
    conn.commit()
    print("✅ Inserted data into leads table")
    cur.close()
    conn.close()
if __name__ == "__main__":
    main()

               
