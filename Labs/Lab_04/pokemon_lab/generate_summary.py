import pandas as pd
import os
import sys

def generate_summary(portfolio_file):
    """
    Reads a portfolio CSV, calculates key metrics, and prints a summary report.
    """
    # 1. Check if the file exists
    if not os.path.exists(portfolio_file):
        print(f"Error: Portfolio file not found at '{portfolio_file}'", file=sys.stderr)
        sys.exit(1) # Exit the script with an error code

    # 2. Read the CSV file
    try:
        df = pd.read_csv(portfolio_file)
    except Exception as e:
        print(f"Error: Could not read file '{portfolio_file}': {e}", file=sys.stderr)
        sys.exit(1)

    # 3. Check if the DataFrame is empty
    if df.empty:
        print(f"Portfolio '{portfolio_file}' contains no data. Nothing to summarize.")
        return # Exit the function cleanly

    # 4. Calculate Total Value
    # Ensure the column exists before trying to sum it
    if 'card_market_value' in df.columns:
        total_portfolio_value = df['card_market_value'].sum()
    else:
        print("Error: 'card_market_value' column not found. Cannot calculate total value.", file=sys.stderr)
        total_portfolio_value = 0 # Default to 0

    # 5. Find Most Valuable Card
    # Also check that the column exists and the dataframe is not empty
    if 'card_market_value' in df.columns and not df.empty:
        most_valuable_card = df.loc[df['card_market_value'].idxmax()]
    else:
        most_valuable_card = None # Set to None if we can't find it

    # 6. Print Report
    print("-" * 40)
    print(f"Portfolio Summary for: {portfolio_file}")
    print("-" * 40)
    
    # Use an f-string to format the currency value
    print(f"Total Portfolio Value: ${total_portfolio_value:,.2f}")
    
    if most_valuable_card is not None:
        print("\n--- Most Valuable Card ---")
        print(f"Name:  {most_valuable_card['card_name']}")
        print(f"ID:    {most_valuable_card['card_id']}")
        print(f"Value: ${most_valuable_card['card_market_value']:,.2f}")
    else:
        print("\nCould not determine the most valuable card.")
        
    print("-" * 40)


# --- Public Interface Functions ---

def main():
    """
    Public function to run the summary on the PRODUCTION portfolio.
    """
    generate_summary('card_portfolio.csv')

def test():
    """
    Public function to run the summary on the TEST portfolio.
    """
    generate_summary('test_card_portfolio.csv')


# --- Main Block ---

if __name__ == "__main__":
    """
    This block runs when the script is executed directly.
    The default behavior is to run the summary on the test data.
    """
    print("Defaulting to Test Mode.", file=sys.stderr)
    test()
