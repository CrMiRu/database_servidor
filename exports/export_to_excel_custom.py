import pandas as pd
import psycopg2
import sys
import os
from pathlib import Path
from datetime import datetime

# 1. Setup Paths
BASE_DIR = Path(__file__).resolve().parent.parent
EXPORTS_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))

from database.connection import load_db_config


def list_server_contents():
    """Prints all databases and tables available to the user."""
    db_config = load_db_config()
    # Connect to the default 'postgres' database to see the list of other databases
    db_config["dbname"] = "postgres"

    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # A. List all Databases
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        databases = [row[0] for row in cur.fetchall()]

        print("\n=== 🖥️ DATABASES ON SERVER ===")
        for db in databases:
            print(f"📁 {db}")

            # B. List Tables for each database
            # We must connect to each DB specifically to see its tables
            try:
                temp_config = db_config.copy()
                temp_config["dbname"] = db
                with psycopg2.connect(**temp_config) as conn_inner:
                    with conn_inner.cursor() as cur_inner:
                        cur_inner.execute(
                            """
                            SELECT table_name 
                            FROM information_schema.tables 
                            WHERE table_schema = 'public'
                        """
                        )
                        tables = cur_inner.fetchall()
                        if tables:
                            for table in tables:
                                print(f"   └── 📄 {table[0]}")
                        else:
                            print("   └── (No public tables)")
            except Exception:
                print("   └── ⚠️ (Access Denied/Connection Error)")

        print("=" * 30 + "\n")

    except Exception as e:
        print(f"❌ Error listing server contents: {e}")
    finally:
        if conn:
            conn.close()


def export_custom_table():
    # Show the user what's available first
    list_server_contents()

    target_db = input("Enter the Database Name to export from: ").strip()
    target_table = input("Enter the Table Name to export: ").strip()

    db_config = load_db_config()
    db_config["dbname"] = target_db

    conn, cur = None, None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # Fetch Data and Columns
        cur.execute(f"SELECT * FROM {target_table};")
        columns = [desc[0] for desc in cur.description]
        data = cur.fetchall()

        if not data:
            print(f"⚠️ Table '{target_table}' is empty.")
            return

        # 1. Create DataFrame
        df = pd.DataFrame(data, columns=columns)

        # --- Clean timezones from data in order to export to Excel (timezones are not supported) ---
        # We loop through each column to find datetimes with timezones
        for col in df.select_dtypes(include=["datetimetz", "datetime"]):
            # This removes the timezone (e.g., +02:00) but keeps the local time
            df[col] = df[col].dt.tz_localize(None)

        # 2. Save to Excel
        timestamp = datetime.now().strftime("%Y-%m-%d")
        filename = f"custom_{target_db}_{target_table}_{timestamp}.xlsx"
        output_path = EXPORTS_DIR / filename

        df.to_excel(output_path, index=False, engine="openpyxl")

        print(f"\n✅ Success! Data from '{target_db}.{target_table}' exported to:")
        print(f"📍 {output_path}")

    except Exception as e:
        print(f"❌ Error during export: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    export_custom_table()
