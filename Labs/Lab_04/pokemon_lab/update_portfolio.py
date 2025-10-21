# ...existing code...
def _load_lookup_data(lookup_dir):
    """
    Load JSON lookup files from lookup_dir, flatten them and return a DataFrame
    with one row per card_id containing the highest available market price.

    Returns: pd.DataFrame with columns:
      ['card_id', 'set_id', 'set_name', 'name', 'card_market_value']
    """
    import json
    from pathlib import Path
    import pandas as pd

    all_lookup_df = []
    lookup_path = Path(lookup_dir)

    for filepath in lookup_path.glob("*.json"):
        try:
            with filepath.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception:
            # skip files we can't read/parse
            continue

        records = data.get("data", []) or []
        if not records:
            continue

        # flatten nested JSON; nested keys use dot notation
        df = pd.json_normalize(records, sep=".")

        # Price calculation: prefer holofoil.market, then normal.market, then 0.0
        holo_col = "tcgplayer.prices.holofoil.market"
        normal_col = "tcgplayer.prices.normal.market"

        # start with zeros
        df["card_market_value"] = 0.0

        if holo_col in df.columns:
            df["card_market_value"] = df[holo_col].fillna(0.0)

        if normal_col in df.columns:
            # where holo is zero/missing, use normal
            df["card_market_value"] = df["card_market_value"].where(
                df["card_market_value"] != 0.0, df[normal_col].fillna(0.0)
            )

        # Standardize column names
        df = df.rename(columns={"id": "card_id", "set.id": "set_id", "set.name": "set_name", "name": "name"})

        required_cols = ["card_id", "set_id", "set_name", "name", "card_market_value"]
        # ensure required cols exist
        for c in required_cols:
            if c not in df.columns:
                df[c] = pd.NA if c != "card_market_value" else 0.0

        all_lookup_df.append(df[required_cols].copy())

    if not all_lookup_df:
        return pd.DataFrame(columns=["card_id", "set_id", "set_name", "name", "card_market_value"])

    lookup_df = pd.concat(all_lookup_df, ignore_index=True)

    # keep the row with the highest market value per card_id
    lookup_df = lookup_df.sort_values("card_market_value", ascending=False)
    lookup_df = lookup_df.drop_duplicates(subset=["card_id"], keep="first").reset_index(drop=True)

    return lookup_df

#Function 2

def _load_inventory_data(inventory_dir):
    """
    Load CSV inventory files from inventory_dir and return a DataFrame
    with a normalized 'card_id' column that matches the lookup (e.g. "base4-4").
    Returned columns include:
      ['card_id','card_name','set_id','card_number','binder_name','page_number','slot_number']
    """
    import pandas as pd
    from pathlib import Path

    inventory_path = Path(inventory_dir)
    all_inv = []

    for csv_file in inventory_path.glob("*.csv"):
        try:
            df = pd.read_csv(csv_file, dtype=str).fillna("")
        except Exception:
            # skip unreadable files
            continue

        # normalize column names if needed
        df = df.rename(columns={
            "card_name": "card_name",
            "set_id": "set_id",
            "card_number": "card_number",
            "binder_name": "binder_name",
            "page_number": "page_number",
            "slot_number": "slot_number",
            # allow alternate headers if present
            "number": "card_number",
            "name": "card_name",
        })

        # ensure required cols exist
        for c in ["card_name", "set_id", "card_number", "binder_name", "page_number", "slot_number"]:
            if c not in df.columns:
                df[c] = ""

        # build card_id to match lookup format: "<set_id>-<card_number>"
        df["card_number"] = df["card_number"].astype(str).str.strip()
        df["set_id"] = df["set_id"].astype(str).str.strip()
        df["card_id"] = df["set_id"] + "-" + df["card_number"]

        # keep and reorder columns
        cols = ["card_id", "card_name", "set_id", "card_number", "binder_name", "page_number", "slot_number"]
        all_inv.append(df[cols].copy())

    if not all_inv:
        return pd.DataFrame(columns=["card_id","card_name","set_id","card_number","binder_name","page_number","slot_number"])

    inv_df = pd.concat(all_inv, ignore_index=True)
    return inv_df

# Function 3

def update_portfolio(inventory_dir, lookup_dir, output_file):
    """
    Orchestrate ETL: load inventory and lookup data, merge, clean, and write portfolio CSV.

    Params:
      inventory_dir (str or Path) - directory with CSV inventory files
      lookup_dir (str or Path)    - directory with JSON lookup files
      output_file (str or Path)   - path to write final portfolio CSV

    Behavior:
      - Calls _load_inventory_data() and _load_lookup_data()
      - If inventory is empty: write empty CSV with headers, print error to stderr and return
      - Left-joins lookup (set_name, card_market_value) onto inventory on card_id
      - Fills missing values, creates 'index' location column, writes CSV, prints success
    """
    import sys
    from pathlib import Path
    import pandas as pd

    inventory_df = _load_inventory_data(inventory_dir)
    lookup_df = _load_lookup_data(lookup_dir)

    # Required output columns
    final_cols = [
        "card_id",
        "card_name",
        "set_id",
        "card_number",
        "binder_name",
        "page_number",
        "slot_number",
        "set_name",
        "card_market_value",
        "index",
    ]

    # Handle empty inventory
    if inventory_df.empty:
        print("Error: inventory is empty. Writing empty portfolio with headers.", file=sys.stderr)
        empty_df = pd.DataFrame(columns=final_cols)
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        empty_df.to_csv(output_file, index=False)
        return

    # Prepare lookup subset to merge
    lookup_sub = lookup_df[["card_id", "set_name", "card_market_value"]] if not lookup_df.empty else pd.DataFrame(columns=["card_id", "set_name", "card_market_value"])

    # Merge (left join to keep all inventory rows)
    merged = pd.merge(inventory_df, lookup_sub, on="card_id", how="left")

    # Final cleaning
    merged["card_market_value"] = merged["card_market_value"].fillna(0.0)
    merged["set_name"] = merged["set_name"].fillna("NOT_FOUND")

    # Ensure location parts are strings and not NaN
    for col in ["binder_name", "page_number", "slot_number"]:
        if col not in merged.columns:
            merged[col] = ""
        merged[col] = merged[col].astype(str).fillna("")

    # Create 'index' column by concatenating binder_name, page_number, slot_number
    merged["index"] = merged["binder_name"].str.strip() + "-" + merged["page_number"].str.strip() + "-" + merged["slot_number"].str.strip()

    # Reorder/select final columns; create any missing final cols to ensure consistent output
    for c in final_cols:
        if c not in merged.columns:
            merged[c] = ""

    final_df = merged[final_cols].copy()

    # Write to CSV
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(output_file, index=False)

    print(f"Wrote portfolio to {output_file}")

# Function 4

def main():
    """Run the pipeline using production directories and output file."""
    prod_inventory = "./card_inventory/"
    prod_lookup = "./card_set_lookup/"
    prod_output = "card_portfolio.csv"
    update_portfolio(prod_inventory, prod_lookup, prod_output)


def test():
    """Run the pipeline using test directories and test output file."""
    test_inventory = "./card_inventory_test/"
    test_lookup = "./card_set_lookup_test/"
    test_output = "test_card_portfolio.csv"
    update_portfolio(test_inventory, test_lookup, test_output)


if __name__ == "__main__":
    import sys

    print("Starting update_portfolio in Test Mode...", file=sys.stderr)
    test()
# ...existing code...
