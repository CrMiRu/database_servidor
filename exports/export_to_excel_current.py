import pandas as pd
import sys
import os
from pathlib import Path
from datetime import datetime

# 1. Get Absolute Paths
BASE_DIR = Path(__file__).resolve().parent.parent
EXPORTS_DIR = Path(__file__).resolve().parent

# 2. Add root to sys.path so we can find the 'database' package
sys.path.append(str(BASE_DIR))

# Import your operations and the config loader to get the DB name
from database.operations import get_all_series
from database.connection import load_db_config


def export_database_to_excel():
    # 3. Fetch data AND column names dynamically from SQL
    data, columns = get_all_series()

    if not data:
        print("⚠️ No data found to export.")
        return

    # 4. Get Database Name from your config
    config = load_db_config()
    db_name = config.get("dbname", "unknown_db")

    # 5. Define Table Name and Timestamp
    table_name = "demo_series"
    timestamp = datetime.now().strftime("%Y-%m-%d")

    # Structure: current_[database]_[table]_[date].xlsx
    filename = f"current_{db_name}_{table_name}_{timestamp}.xlsx"
    OUTPUT_FILE = EXPORTS_DIR / filename

    # 6. Create a DataFrame
    df = pd.DataFrame(data, columns=columns)

    # 7. Save to Excel
    df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")

    print(f"✅ Success! Data exported to: {OUTPUT_FILE}")


if __name__ == "__main__":
    export_database_to_excel()
