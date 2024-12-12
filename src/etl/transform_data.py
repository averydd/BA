def transform_blocks(data):
    return [
        {
            "id": item["_id"],
            "type": item.get("type"),
            "number": item.get("number"),
            "hash": item.get("hash"),
            "parent_hash": item.get("parent_hash"),
            "nonce": item.get("nonce"),
            "miner": item.get("miner"),
            "difficulty": item.get("difficulty"),
            "total_difficulty": item.get("total_difficulty"),
            "size": item.get("size"),
            "gas_limit": item.get("gas_limit"),
            "gas_used": item.get("gas_used"),
            "timestamp": item.get("timestamp"),
            "transaction_count": item.get("transaction_count"),
            "item_timestamp": item.get("item_timestamp"),
        }
        for item in data
    ]

def transform_transactions(data):
    return [
        {
            "id": item["_id"],
            "type": item.get("type"),
            "hash": item.get("hash"),
            "nonce": item.get("nonce"),
            "transaction_index": item.get("transaction_index"),
            "from_address": item.get("from_address"),
            "to_address": item.get("to_address"),
            "value": item.get("value"),
            "gas": item.get("gas"),
            "gas_price": item.get("gas_price"),
            "input": item.get("input"),
            "block_timestamp": item.get("block_timestamp"),
            "block_number": item.get("block_number"),
            "block_hash": item.get("block_hash"),
            "receipt_gas_used": item.get("receipt_gas_used"),
            "receipt_contract_address": item.get("receipt_contract_address"),
            "receipt_status": item.get("receipt_status"),
            "item_timestamp": item.get("item_timestamp"),
        }
        for item in data
    ]

def transform_internal_transactions(data):
    return [
        {
            "id": item["_id"],
            "block_number": item.get("block_number"),
            "hash": item.get("hash"),
            "internal_transactions": item.get("internal_transactions"),
        }
        for item in data
    ]

def transform_lending_events(data):
    return [
        {
            "id": item["_id"],
            "amount": item.get("amount"),
            "block_number": item.get("block_number"),
            "block_timestamp": item.get("block_timestamp"),
            "transaction_hash": item.get("transaction_hash"),
            "contract_address": item.get("contract_address"),
            "event_type": item.get("event_type"),
            "log_index": item.get("log_index"),
            "on_behalf_of": item.get("on_behalf_of"),
            "user": item.get("user"),
            "wallet": item.get("wallet"),
            "reserve": item.get("reserve"),
        }
        for item in data
    ]

def transform_dex_events(data):
    return [
        {
            "id": item["_id"],
            "block_number": item.get("block_number"),
            "amount0": item.get("amount0"),
            "amount1": item.get("amount1"),
            "block_timestamp": item.get("block_timestamp"),
            "contract_address": item.get("contract_address"),
            "event_type": item.get("event_type"),
            "liquidity": item.get("liquidity"),
            "log_index": item.get("log_index"),
            "recipient": item.get("recipient"),
            "sender": item.get("sender"),
            "transaction_hash": item.get("transaction_hash"),
            "wallet": item.get("wallet"),
        }
        for item in data
    ]

def transform_events(data):
    return [
        {
            "id": item["_id"],
            "block_number": item.get("block_number"),
            "amount0": item.get("amount0"),
            "amount1": item.get("amount1"),
            "block_timestamp": item.get("block_timestamp"),
            "contract_address": item.get("contract_address"),
            "event_type": item.get("event_type"),
            "liquidity": item.get("liquidity"),
            "log_index": item.get("log_index"),
            "recipient": item.get("recipient"),
            "sender": item.get("sender"),
            "transaction_hash": item.get("transaction_hash"),
            "wallet": item.get("wallet"),
        }
        for item in data
    ]
