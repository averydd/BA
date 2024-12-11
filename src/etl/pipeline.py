from sqlalchemy import create_engine, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert
from config.database_config import TARGET_POSTGRES_URI
from etl.extract_mongo import extract_mongo_data
from etl.transform_data import transform_blocks, transform_transactions, transform_internal_transactions

def run_etl():
    engine = create_engine(TARGET_POSTGRES_URI)
    metadata = MetaData()
    metadata.reflect(bind=engine)

    # Tables
    blocks_table = metadata.tables['blocks']
    transactions_table = metadata.tables['transactions']
    internal_transactions_table = metadata.tables['internal_transactions']
    token_transfers_table = metadata.tables['token_transfers']
    events_table = metadata.tables['events']

    # Step 1: Extract Data
    mongo_database = "ethereum_blockchain_etl"
    blocks_data = extract_mongo_data(mongo_database, "blocks", {}, 100)
    transactions_data = extract_mongo_data(mongo_database, "transactions", {}, 100)
    internal_transactions_data = extract_mongo_data(mongo_database, "internal_transactions", {}, 100)
    token_transfers_data = extract_mongo_data(mongo_database, "chain_0x1.token_transfer", {}, 100)
    events_data = extract_mongo_data(mongo_database, "ethereum_blockchain_etl.lending_events", {}, 100)

    # Step 2: Transform Data
    transformed_blocks = transform_blocks(blocks_data)
    transformed_transactions = transform_transactions(transactions_data)
    transformed_internal_transactions = transform_internal_transactions(internal_transactions_data)

    # Step 3: Load Data
    with engine.begin() as conn:
        # Load Blocks
        if transformed_blocks:
            conn.execute(pg_insert(blocks_table).on_conflict_do_nothing(), transformed_blocks)

        # Validate Transactions
        valid_transactions = [
            txn for txn in transformed_transactions if txn["block_number"] in {block["number"] for block in transformed_blocks}
        ]
        if valid_transactions:
            conn.execute(pg_insert(transactions_table).on_conflict_do_nothing(), valid_transactions)

        # Load Internal Transactions
        if transformed_internal_transactions:
            conn.execute(pg_insert(internal_transactions_table).on_conflict_do_nothing(), transformed_internal_transactions)

        # Load Token Transfers
        if token_transfers_data:
            conn.execute(pg_insert(token_transfers_table).on_conflict_do_nothing(), token_transfers_data)

        # Load Events
        if events_data:
            conn.execute(pg_insert(events_table).on_conflict_do_nothing(), events_data)
    print("Raw Blocks Data:")
    print(blocks_data[:5])  # Print the first few raw records to inspect

    print("Data successfully loaded into PostgreSQL.")
