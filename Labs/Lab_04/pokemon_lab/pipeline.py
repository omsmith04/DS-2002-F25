import update_portfolio
import generate_summary
import sys

def run_production_pipeline():
    """
    Runs the full production ETL and Reporting pipeline.
    """
    # 1. Print starting message
    print("--- Starting Production Pipeline ---", file=sys.stderr)

    # 2. ETL Step
    print("\nStep 1: Running ETL (update_portfolio.py)...", file=sys.stderr)
    update_portfolio.main() # This runs the production update

    # 3. Reporting Step
    print("\nStep 2: Running Reporting (generate_summary.py)...", file=sys.stderr)
    generate_summary.main() # This runs the production summary

    # 4. Print completion message
    print("\n--- Production Pipeline Complete ---", file=sys.stderr)

# --- Main Block ---
if __name__ == "__main__":
    """
    This block is the single entry point for the pipeline.
    """
    # 5. Call the master function
    run_production_pipeline()
