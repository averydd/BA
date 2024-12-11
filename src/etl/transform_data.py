def transform_blocks(raw_blocks):
    """
    Transforms raw block data from MongoDB into a format suitable for PostgreSQL insertion.
    
    Args:
        raw_blocks (list): List of raw block records extracted from MongoDB.
    
    Returns:
        list: Transformed block data.
    """
    transformed_blocks = []
    for block in raw_blocks:
        transformed_blocks.append({
            "block_number": block.get("number"),
            "block_hash": block.get("hash"),
            "parent_hash": block.get("parent_hash"),
            "nonce": block.get("nonce"),
            "transactions_root": block.get("transactions_root"),
            "state_root": block.get("state_root"),
            "receipts_root": block.get("receipts_root"),
            "miner": block.get("miner"),
            "difficulty": block.get("difficulty"),
            "total_difficulty": block.get("total_difficulty"),
            "size": block.get("size"),
            "gas_limit": block.get("gas_limit"),
            "gas_used": block.get("gas_used"),
            "timestamp": block.get("timestamp"),
            "transaction_count": block.get("transaction_count"),
            "item_timestamp": block.get("item_timestamp"),
        })
    return transformed_blocks

def transform_transactions(raw_transactions):
    """
    Transforms raw transaction data from MongoDB into a format suitable for PostgreSQL insertion.
    
    Args:
        raw_transactions (list): List of raw transaction records extracted from MongoDB.
    
    Returns:
        list: Transformed transaction data.
    """
    transformed_transactions = []
    for transaction in raw_transactions:
        transformed_transactions.append({
            "transaction_hash": transaction.get("hash"),
            "nonce": transaction.get("nonce"),
            "transaction_index": transaction.get("transaction_index"),
            "from_address": transaction.get("from_address"),
            "to_address": transaction.get("to_address"),
            "value": transaction.get("value"),
            "gas": transaction.get("gas"),
            "gas_price": transaction.get("gas_price"),
            "input": transaction.get("input"),
            "block_timestamp": transaction.get("block_timestamp"),
            "block_number": transaction.get("block_number"),
            "block_hash": transaction.get("block_hash"),
            "receipt_cumulative_gas_used": transaction.get("receipt_cumulative_gas_used"),
            "receipt_gas_used": transaction.get("receipt_gas_used"),
            "receipt_contract_address": transaction.get("receipt_contract_address"),
            "receipt_root": transaction.get("receipt_root"),
            "receipt_status": transaction.get("receipt_status"),
            "item_timestamp": transaction.get("item_timestamp"),
        })
    return transformed_transactions

def transform_internal_transactions(raw_internal_transactions):
    """
    Transforms raw internal transaction data from MongoDB into a format suitable for PostgreSQL insertion.
    
    Args:
        raw_internal_transactions (list): List of raw internal transaction records extracted from MongoDB.
    
    Returns:
        list: Transformed internal transaction data.
    """
    transformed_internal_transactions = []
    for record in raw_internal_transactions:
        # Extract internal transactions array
        internal_transactions = record.get("internal_transactions", [])
        for internal_tx in internal_transactions:
            transformed_internal_transactions.append({
                "hash": record.get("hash"),
                "block_number": record.get("block_number"),
                "from_address": internal_tx.get("from"),
                "to_address": internal_tx.get("to"),
                "value": internal_tx.get("value"),
                "contract_address": internal_tx.get("contract_address"),
                "input": internal_tx.get("input"),
                "type": internal_tx.get("type"),
                "gas": internal_tx.get("gas"),
                "gas_used": internal_tx.get("gas_used"),
                "trace_id": internal_tx.get("trace_id"),
                "is_error": internal_tx.get("is_error"),
                "err_code": internal_tx.get("err_code"),
            })
    return transformed_internal_transactions
