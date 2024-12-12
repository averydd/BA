from pymongo import MongoClient
from config.database_config import MONGO_URI

def connect_mongo(uri=MONGO_URI):
    """
    Connect to MongoDB using the provided URI.
    """
    try:
        client = MongoClient(uri)
        print("MongoDB connected!")
        return client
    except Exception as e:
        print(f"MongoDB connection error: {e}")
        return None

def extract_mongo_data(database_name, collection_name, query={}, limit=100):
    """
    Extract data from a specific MongoDB collection.
    """
    client = connect_mongo()
    if client is None:
        return []

    try:
        db = client[database_name]
        collection = db[collection_name]
        data = list(collection.find(query).limit(limit))
        print(f"Extracted {len(data)} records from {database_name}.{collection_name}")
        return data
    except Exception as e:
        print(f"Error querying {collection_name} in {database_name}: {e}")
        return []
    finally:
        client.close()

"""def extract_transactions_with_blocks(mongo_database, block_field_value):
    """
    Fetches transactions filtered by a related field in the blocks collection.

    Parameters:
    - mongo_database: The database object.
    - block_field_value: The field value to match in the blocks collection.

    Returns:
    - A list of matching transactions.
    """
    transactions_collection = mongo_database["transactions"]
    blocks_collection = mongo_database["blocks"]
    pipeline = [
        {
            "$lookup": {
                "from": "blocks",  # The related collection
                "localField": "block_number",  # Field in transactions
                "foreignField": "number",  # Field in blocks
                "as": "block_info"  # Name for the joined data
            }
        },
        {
            "$match": {
                "block_info.block_field": block_field_value  # Replace 'block_field' with the actual field
            }
        },
        {
            "$project": {"block_info": 0}  # Optionally exclude joined data from the result
    }]
    
    return list(transactions_collection.aggregate(pipeline))

# Use the function
block_field_value = "desired_value"
mongo_transactions_data = extract_transactions_with_blocks(mongo_database, block_field_value)
"""
