from etl.extract_mongo import extract_mongo_data
from etl.transform_data import transform_blocks
from etl.load_postgres import load_data_to_postgres
from sqlalchemy import create_engine
from config.database_config import TARGET_POSTGRES_URI

def run_etl():
    # Database configurations
    MONGO_DATABASE = "ethereum_blockchain_etl"
    COLLECTION = "blocks"
    TARGET_TABLE = "processed_blocks"

    # Step 1: Extract
    raw_data = extract_mongo_data(MONGO_DATABASE, COLLECTION, {}, 100)
    print("Extracted data:")
    print(raw_data[:5])  # Print the first few records for debugging
    
    if not raw_data:
        print("No data extracted from MongoDB.")
        return

    # Step 2: Transform
    transformed_data = transform_blocks(raw_data)
    print("Transformed data:")
    print(transformed_data[:5])  # Print the first few transformed records for debugging

    # Step 3: Load
    from sqlalchemy import create_engine, MetaData, Table, insert

    engine = create_engine(TARGET_POSTGRES_URI)
    metadata = MetaData()
    processed_blocks = Table("processed_blocks", metadata, autoload_with=engine)

    # Manually insert a test record
    with engine.connect() as conn:
        test_data = [
            {
                "block_number": 1,
                "block_hash": "0x123",
                "miner": "test_miner",
                "timestamp": 1620000000,
                "transaction_count": 10,
            }
        ]
        conn.execute(processed_blocks.insert(), test_data)

    print("Test record inserted.")

