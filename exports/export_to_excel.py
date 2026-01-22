# export the table to an excel file

import pandas as pd
import sys
import os
from pathlib import Path
from datetime import datetime

# 1. Get Absolute Paths using your preferred method
# Path(__file__) is the script, .parent is 'exports' folder, .parent again is root
BASE_DIR = Path(__file__).resolve().parent.parent
EXPORTS_DIR = Path(__file__).resolve().parent

timestamp = datetime.now().strftime("%Y%m%d")
filename = f"series_export_{timestamp}.xlsx"
OUTPUT_FILE = EXPORTS_DIR / filename

# 2. Add root to sys.path so we can find the 'database' package
sys.path.append(str(BASE_DIR))
from database.operations import get_all_series


def export_database_to_excel():
    # 3. Fetch data AND column names dynamically from SQL
    # This ensures Excel headers match your SQL table automatically
    data, columns = get_all_series()

    if not data:
        print("No data found to export.")
        return

    # 4. Create a DataFrame
    df = pd.DataFrame(data, columns=columns)

    # 5. Save to Excel using the Path object
    # index=False removes the extra row numbers column
    df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")

    print(f"Data exported to: {OUTPUT_FILE}")


if __name__ == "__main__":
    export_database_to_excel()
