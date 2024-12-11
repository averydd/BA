import matplotlib.pyplot as plt
import pandas as pd

def plot_transaction_distribution(transactions):
    """
    Plot a histogram of transaction values.
    """
    df = pd.DataFrame(transactions)
    plt.figure(figsize=(10, 6))
    df['value'].plot(kind='hist', bins=20, color='blue', alpha=0.7)
    plt.title('Transaction Value Distribution')
    plt.xlabel('Transaction Value')
    plt.ylabel('Frequency')
    plt.show()

def plot_liquidity_trends(block_data):
    """
    Plot liquidity trends over blocks.
    """
    df = pd.DataFrame(block_data)
    plt.figure(figsize=(10, 6))
    plt.plot(df['block_number'], df['liquidity_ratio'], label='Liquidity Ratio')
    plt.title('Liquidity Trends Over Blocks')
    plt.xlabel('Block Number')
    plt.ylabel('Liquidity Ratio')
    plt.legend()
    plt.show()
