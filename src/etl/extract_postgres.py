import psycopg2
from config.database_config import POSTGRES_URI

def connect_postgres(uri=POSTGRES_URI):
    try:
        conn = psycopg2.connect(uri)
        print("PostgreSQL connected!")
        return conn
    except Exception as e:
        print(f"PostgreSQL connection error: {e}")
        return None

def extract_postgres_data(query, conn=None):
    if conn is None:
        conn = connect_postgres()
    if conn is None:
        return []
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        data = cursor.fetchall()
        print(f"Extracted {len(data)} records from PostgreSQL")
        return data
    except Exception as e:
        print(f"Error executing query: {e}")
        return []
    finally:
        cursor.close()
