import pandas as pd

def analyze_liquidity(block_data):
    """
    Analyze liquidity trends from block data.
    """
    df = pd.DataFrame(block_data)
    df['liquidity_ratio'] = df['gas_used'] / df['gas_limit']
    high_liquidity = df[df['liquidity_ratio'] > 0.5]  # Example threshold
    print(f"Identified {len(high_liquidity)} blocks with high liquidity ratios.")
    return high_liquidity
