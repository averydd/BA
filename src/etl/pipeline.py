from etl.extract_postgres import extract_postgres_data, connect_postgres
from etl.extract_mongo import extract_mongo_data
from etl.transform_data import transform_blocks, transform_transactions, transform_internal_transactions, transform_lending_events, transform_dex_events, transform_events
from etl.load_postgres import load_data_to_postgres
from sqlalchemy import create_engine, MetaData
from config.database_config import TARGET_POSTGRES_URI
from sqlalchemy.sql import text

def test_database_connection(engine):
    """
    Tests the connection to the database by querying the current database name.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT current_database();"))
            db_name = result.scalar()
            print(f"Connected to database: {db_name}")
    except Exception as e:
        print(f"Error checking database connection: {e}")

def run_etl():
    """
    Runs the ETL pipeline to extract, transform, and load data from MongoDB and PostgreSQL.
    """
    # PostgreSQL connection setup
    engine = create_engine(TARGET_POSTGRES_URI)
    test_database_connection(engine)
    postgres_conn = connect_postgres()
    if postgres_conn is None:
        print("Failed to connect to PostgreSQL. Exiting ETL pipeline.")
        return

    # Step 1: Extract from MongoDB
    recs = 10
    mongo_database = "ethereum_blockchain_etl"
    mongo_blocks_data = extract_mongo_data(mongo_database, "blocks", {}, recs)
    mongo_transactions_data = extract_mongo_data(mongo_database, "transactions", {}, recs)
    mongo_internal_transactions_data = extract_mongo_data(mongo_database, "internal_transactions", {}, recs)
    mongo_events_data = extract_mongo_data(mongo_database, "events", {}, recs)
    mongo_lending_events_data = extract_mongo_data(mongo_database, "lending_events", {}, recs)
    mongo_dex_events_data = extract_mongo_data(mongo_database, "dex_events", {}, recs)

    # Step 2: Extract from PostgreSQL
    token_transfers_query = "SELECT * FROM chain_0x1.token_transfer LIMIT 10"
    try:
        token_transfers_data = extract_postgres_data(token_transfers_query, postgres_conn)
    except Exception as e:
        print(f"Error extracting token_transfers data: {e}")
        token_transfers_data = None

    # Step 3: Transform
    transformed_blocks = transform_blocks(mongo_blocks_data)
    transformed_transactions = transform_transactions(mongo_transactions_data)
    transformed_internal_transactions = transform_internal_transactions(mongo_internal_transactions_data)
    transformed_lending_events = transform_lending_events(mongo_lending_events_data)
    transformed_dex_events = transform_dex_events(mongo_dex_events_data)
    transformed_events = transform_events(mongo_events_data)
    # Step 4: Load data
    try:
        load_data_to_postgres(transformed_blocks, "blocks", engine)
        load_data_to_postgres(transformed_transactions, "transactions", engine)
        load_data_to_postgres(transformed_internal_transactions, "internal_transactions", engine)
        load_data_to_postgres()
        if token_transfers_data:
            load_data_to_postgres(token_transfers_data, "token_transfers", engine)
    except Exception as e:
        print(f"Error loading data: {e}")

    # Close PostgreSQL connection
    postgres_conn.close()
    print("ETL pipeline successfully completed.")
