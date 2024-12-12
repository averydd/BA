from sqlalchemy import Table, MetaData, Column, Integer, String, create_engine
from sqlalchemy.exc import IntegrityError

def create_table_if_not_exists(engine, table_name, columns):
    """
    Create a table in PostgreSQL if it does not exist.

    Parameters:
        engine (Engine): SQLAlchemy engine for the database.
        table_name (str): Name of the table to ensure existence.
        columns (list[Column]): List of SQLAlchemy Column definitions.
    """
    metadata = MetaData()
    table = Table(table_name, metadata, *columns)
    metadata.create_all(engine)
    print(f"Ensured table {table_name} exists.")

def load_data_to_postgres(data, table_name, engine):
    """
    Loads data into a PostgreSQL table using SQLAlchemy.

    Parameters:
        data (list[dict]): List of dictionaries to insert into the database.
        table_name (str): The name of the table to insert the data into.
        engine (Engine): SQLAlchemy engine for the database.
    """
    try:
        with engine.begin() as conn:
            metadata = MetaData()
            metadata.reflect(bind=engine)
            print(f"Processing Table '{table_name}' ")
            if table_name not in metadata.tables:
                print(f"Table '{table_name}' does not exist.")
                return
            table = metadata.tables[table_name]
            conn.execute(table.insert(), data)
            print(f"Inserted {len(data)} records into '{table_name}'.")
    except Exception as e:
        print(f"Error loading data into '{table_name}': {e}")

