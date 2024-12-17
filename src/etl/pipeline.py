from etl.extract_postgres import extract_postgres_data, connect_postgres
from etl.extract_mongo import extract_mongo_data
from etl.transform_data import (
    transform_blocks, transform_transactions,
    transform_internal_transactions, transform_lending_events,
    transform_dex_events, transform_events
)
from etl.load_postgres import load_data_to_postgres
from sqlalchemy import create_engine
from config.database_config import TARGET_POSTGRES_URI
from sqlalchemy.sql import text
import pandas as pd

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

    # Step 1: Extract data
    recs = 1000
    mongo_database = "ethereum_blockchain_etl"
    mongo_blocks_data = extract_mongo_data(mongo_database, "blocks", {}, recs)
    mongo_transactions_data = extract_mongo_data(mongo_database, "transactions", {}, recs)

    token_transfers_query = "SELECT * FROM chain_0x1.token_transfer LIMIT 10"
    try:
        token_transfers_data = extract_postgres_data(token_transfers_query, postgres_conn)
    except Exception as e:
        print(f"Error extracting token_transfers data: {e}")
        token_transfers_data = pd.DataFrame()  # Empty DataFrame if extraction fails

    # Step 2: Transform and Load Blocks
    try:
        transformed_blocks_df = pd.DataFrame(transform_blocks(mongo_blocks_data))

        if transformed_blocks_df.empty:
            print("No block data to load. Exiting ETL pipeline.")
            return
        if 'number' not in transformed_blocks_df.columns:
            print("Error: 'number' column missing in blocks data.")
            return

        load_data_to_postgres(transformed_blocks_df.to_dict(orient='records'), "blocks", engine)
        print("Blocks data loaded successfully.")
    except Exception as e:
        print(f"Error loading blocks data: {e}")
        return

    # Step 3: Transform and Load Transactions
    transformed_blocks_df.rename(columns={'number': 'block_number'}, inplace=True)
    try:
        transformed_transactions_df = pd.DataFrame(transform_transactions(mongo_transactions_data))
        if transformed_transactions_df.empty or 'block_number' not in transformed_transactions_df.columns:
            print("No valid transactions data to load. Exiting ETL pipeline.")
            return

        # Filter transactions with valid block_numbers
        valid_block_numbers = set(transformed_blocks_df['block_number'])
        filtered_transactions_df = transformed_transactions_df[
            transformed_transactions_df['block_number'].isin(valid_block_numbers)
        ]

        if not filtered_transactions_df.empty:
            load_data_to_postgres(filtered_transactions_df.to_dict(orient='records'), "transactions", engine)
            print("Transactions data loaded successfully.")
        else:
            print("No transactions data matched valid block numbers.")
    except Exception as e:
        print(f"Error loading transactions data: {e}")
    #print("Block numbers in transformed_blocks_df:")
    #print(transformed_blocks_df['block_number'].unique())

    #print("Block numbers in transformed_transactions_df:")
    #print(transformed_transactions_df['block_number'].unique())

    # Step 4: Load Token Transfers
    try:
        if token_transfers_data:
            token_transfers_df = pd.DataFrame(token_transfers_data)
            print("Columns in token_transfers data:")
            print(token_transfers_df.columns)
            if not token_transfers_df.empty:
                load_data_to_postgres(token_transfers_df.to_dict(orient='records'), "token_transfers", engine)
                print("Token transfers data loaded successfully.")
            else:
                print("No valid token transfers data to load.")
    except Exception as e:
        print(f"Error loading token transfers data: {e}")

    # Close PostgreSQL connection
    postgres_conn.close()
    print("ETL pipeline successfully completed.")
