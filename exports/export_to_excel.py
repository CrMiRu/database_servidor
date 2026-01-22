# export the table to an excel file

import pandas as pd
import sys
import os

# This allows the script to find the 'database' folder in the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.operations import get_all_series


def export_database_to_excel():
    # 1. Fetch data using your existing operation
    data = get_all_series()

    if not data:
        print("No data found to export.")
        return

    # 2. Define the column headers (matching your SQL table)
    columns = [
        "ID",
        "Title",
        "Genre",
        "Seasons",
        "Rating",
        "Release Year",
        "Streaming Platform",
    ]

    # 3. Create a DataFrame (Pandas table)
    df = pd.DataFrame(data, columns=columns)

    # 4. Save to Excel
    filename = "series_export.xlsx"
    df.to_excel(filename, index=False, engine="openpyxl")

    print(f"Success! Data exported to {filename}")


if __name__ == "__main__":
    export_database_to_excel()
