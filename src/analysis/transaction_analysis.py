import pandas as pd

def analyze_transactions(transactions):
    """
    Analyze token transfers for high-value transactions.
    """
    df = pd.DataFrame(transactions)
    # Filter whale transactions (e.g., value > 1000)
    whales = df[df['value'] > 1000]
    print(f"Found {len(whales)} whale transactions.")
    return whales

def summarize_transactions(transactions):
    """
    Summarize transaction data for reporting.
    """
    df = pd.DataFrame(transactions)
    summary = df['value'].describe()  # Summary statistics
    print("Transaction Summary:")
    print(summary)
    return summary
