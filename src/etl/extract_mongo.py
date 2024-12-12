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
