from sqlalchemy import Table, MetaData, Column, Integer, String, create_engine
from sqlalchemy.exc import IntegrityError

def create_table_if_not_exists(engine, table_name):
    """
    Create a table in PostgreSQL if it does not exist.
    """
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("block_number", Integer, primary_key=True),
        Column("block_hash", String),
        Column("miner", String),
        Column("timestamp", Integer),
        Column("transaction_count", Integer),
    )
    metadata.create_all(engine)
    print(f"Ensured table {table_name} exists.")

def load_data_to_postgres(data, table_name, engine):
    """
    Load transformed data into a PostgreSQL table.
    """
    create_table_if_not_exists(engine, table_name)
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    try:
        with engine.connect() as conn:
            conn.execute(table.insert(), data)
            print(f"Loaded {len(data)} records into {table_name}")
    except IntegrityError as e:
        print(f"Data loading error: {e}")
