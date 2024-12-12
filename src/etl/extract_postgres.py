import psycopg2
from psycopg2.extras import RealDictCursor
from config.database_config import POSTGRES_URI

def connect_postgres(uri=POSTGRES_URI):
    """
    Establishes a connection to the PostgreSQL database.
    """
    try:
        conn = psycopg2.connect(uri)
        print("PostgreSQL connected!")
        return conn
    except Exception as e:
        print(f"PostgreSQL connection error: {e}")
        return None

def extract_postgres_data(query, conn=None):
    """
    Executes a SQL query to extract data from PostgreSQL.
    """
    if conn is None:
        conn = connect_postgres()
    if conn is None:
        return []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            data = cursor.fetchall()
            print(f"Extracted {len(data)} records from PostgreSQL.")
            return data
    except Exception as e:
        print(f"Error executing query: {e}")
        return []
