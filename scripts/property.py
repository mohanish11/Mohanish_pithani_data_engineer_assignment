from utils import get_connection
def create_property_table(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS home_db.property (
    property_id           INT PRIMARY KEY,
    Property_Title         LONGTEXT,
    Address                LONGTEXT,
    Market                 LONGTEXT,
    Flood                  LONGTEXT,
    Street_Address         LONGTEXT,
    City                   LONGTEXT,
    State                  LONGTEXT,
    Zip                    LONGTEXT,
    Property_Type          LONGTEXT,
    Highway                LONGTEXT,
    Train                  LONGTEXT,
    Tax_Rate               DECIMAL(10,2),
    SQFT_Basement          INT UNSIGNED,
    HTW                    LONGTEXT,
    Pool                   LONGTEXT,
    Commercial             LONGTEXT,
    Water                  LONGTEXT,
    Sewage                 LONGTEXT,
    Year_Built             INT UNSIGNED,
    SQFT_MU                INT UNSIGNED,
    SQFT_Total             LONGTEXT,
    Parking                LONGTEXT,
    Bed                    INT UNSIGNED,
    Bath                   INT UNSIGNED,
    BasementYesNo          LONGTEXT,
    Layout                 LONGTEXT,
    Rent_Restricted        LONGTEXT,
    Neighborhood_Rating    INT,
    Latitude               DECIMAL(10,6),
    Longitude              DECIMAL(10,6),
    Subdivision            LONGTEXT,
    School_Average         DECIMAL(10,2)
);
    """)
def insert_into_property(cur):
    cur.execute("""
               INSERT INTO home_db.property (
    property_id,
    Property_Title,
    Address,
    Market,
    Flood,
    Street_Address,
    City,
    State,
    Zip,
    Property_Type,
    Highway,
    Train,
    Tax_Rate,
    SQFT_Basement,
    HTW,
    Pool,
    Commercial,
    Water,
    Sewage,
    Year_Built,
    SQFT_MU,
    SQFT_Total,
    Parking,
    Bed,
    Bath,
    BasementYesNo,
    Layout,
    Rent_Restricted,
    Neighborhood_Rating,
    Latitude,
    Longitude,
    Subdivision,
    School_Average
)
SELECT
    raw_id AS property_id,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Property_Title'))       AS Property_Title,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Address'))              AS Address,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Market'))               AS Market,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Flood'))                AS Flood,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Street_Address'))       AS Street_Address,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.City'))                 AS City,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.State'))                AS State,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Zip'))                  AS Zip,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Property_Type'))        AS Property_Type,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Highway'))              AS Highway,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Train'))                AS Train,

    CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Tax_Rate')) AS DECIMAL(10,2))      AS Tax_Rate,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.SQFT_Basement')) AS UNSIGNED)      AS SQFT_Basement,

    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.HTW'))                  AS HTW,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Pool'))                 AS Pool,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Commercial'))           AS Commercial,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Water'))                AS Water,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Sewage'))               AS Sewage,

    CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Year_Built')) AS UNSIGNED)         AS Year_Built,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.SQFT_MU')) AS UNSIGNED)            AS SQFT_MU,

    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.SQFT_Total'))           AS SQFT_Total,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Parking'))              AS Parking,

    CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Bed')) AS UNSIGNED)                AS Bed,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Bath')) AS UNSIGNED)               AS Bath,

    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.BasementYesNo'))        AS BasementYesNo,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Layout'))               AS Layout,
    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Rent_Restricted'))      AS Rent_Restricted,

    CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Neighborhood_Rating')) AS SIGNED)  AS Neighborhood_Rating,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Latitude')) AS DECIMAL(10,6))      AS Latitude,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Longitude')) AS DECIMAL(10,6))     AS Longitude,

    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Subdivision'))          AS Subdivision,
    CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.School_Average')) AS DECIMAL(10,2)) AS School_Average

FROM home_db.raw_property rp
WHERE NOT EXISTS (
    SELECT 1 FROM home_db.property p WHERE p.property_id = rp.raw_id
);
    """)

def main():
    conn = get_connection()
    cur = conn.cursor()
    create_property_table(cur)
    insert_into_property(cur)
    conn.commit()
    print("✅ Inserted data into property table")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
