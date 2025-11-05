import mysql.connector

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "db_user",
    "password": "6equj5_db_user",
    "database": "home_db"
}

def get_connection():
    #Return a MySQL connection.
    return mysql.connector.connect(**DB_CONFIG)
