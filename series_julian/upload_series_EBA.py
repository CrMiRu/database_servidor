"""
This script processes the Excel file 'EBA_series_julian.xlsx' and uploads the data into the PostgreSQL database.
It reads the hierarchical structure from 'estructura_EBA.yaml' and matches it with the Excel data.
The data is organized by date, country, and metric, and is inserted into the appropriate tables in the database.


Table "hierarchy": defines tree structure/folders --> Key Column: "id" --> References: N/A
Table "metrics": defines the specific series (e.g.: ROE for Spain) --> Key Column: "hierarchy_id" --> References: hierarchy.id
Table "values": stores the actual values and dates for each metric --> Key Column: "metric_id" --> References: metrics.id

SQL Query to join "values" and "metrics" tables:
        SELECT
            v.*,
            m.name, m.id
        FROM values v
        JOIN metrics m
            ON v.metric_id = m.id;
"""

import pandas as pd
import json
import yaml
import sys
import numpy as np
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from database.connection import get_db_connection


def upload_values(df_chunk, cur, yaml_section, date):
    """Matches YAML metrics against Excel rows and uploads values."""
    for key, val in yaml_section.items():
        if isinstance(val, dict):
            upload_values(df_chunk, cur, val, date)
        else:
            parts = key.split(".")
            metric_name, country_code = parts[1], parts[2]
            try:
                raw_val = df_chunk.loc[(country_code, metric_name), "valor"]
                # Convert percentages/strings or NumPy types to native Python float
                clean_val = (
                    float(raw_val.replace(",", ".").replace("%", "")) / 100
                    if isinstance(raw_val, str)
                    else float(raw_val)
                )

                if np.isnan(clean_val):
                    continue

                cur.execute("SELECT id FROM metrics WHERE name = %s", (key,))
                m_id = cur.fetchone()
                if m_id:
                    cur.execute(
                        """
                        INSERT INTO values (date, value, metric_id, value_meta)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (metric_id, date) DO UPDATE SET value = EXCLUDED.value;
                    """,
                        (date, clean_val, m_id[0], json.dumps({})),
                    )
            except (KeyError, ValueError):
                pass


def main():
    # INTERACTIVE MODE: Ask for database if not provided as argument
    if len(sys.argv) > 1:
        target_db = sys.argv[1]
    else:
        target_db = input(
            "Choose database to update (leave blank for default): "
        ).strip()
        if not target_db:
            target_db = None

    current_dir = Path(__file__).parent
    data_file = current_dir / "excels_raw_EBA" / "EBA_series_julian.xlsx"
    yaml_file = current_dir / "estructura_EBA.yaml"

    with open(yaml_file, "r", encoding="utf-8") as f:
        yaml_data = yaml.safe_load(f)

    print("📖 Reading Excel...")
    df = pd.read_excel(data_file, sheet_name="KRIs_by_country_and_EU")
    df = df.set_index(["periodo", "pais", "metric"])

    conn, cur = None, None
    try:
        conn, cur = get_db_connection(target_db=target_db)
        periods = df.index.get_level_values("periodo").unique()
        for p in periods:
            dt = datetime.strptime(str(p), "%Y%m").date()
            print(f"🚀 Processing: {dt}")
            upload_values(df.loc[p], cur, yaml_data, dt)
        conn.commit()
        print(f"✅ Data upload complete in '{target_db if target_db else 'default'}'.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"❌ Error: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
