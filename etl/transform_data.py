def transform_blocks(data):
    """
    Transform raw block data for loading into PostgreSQL.
    """
    transformed_data = []
    for record in data:
        transformed_data.append({
            "block_number": record.get("number"),
            "block_hash": record.get("hash"),
            "miner": record.get("miner"),
            "timestamp": record.get("timestamp"),
            "transaction_count": record.get("transaction_count"),
        })
    print(f"Transformed {len(transformed_data)} block records")
    return transformed_data

