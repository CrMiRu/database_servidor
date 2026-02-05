import pandas as pd
import json
import yaml
import sys
import numpy as np
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from database.connection import get_db_connection

CODES = {
    "SAN": "Santander",
    "BBVA": "BBVA",
    "CABK": "CaixaBank",
    "SAB": "Sabadell",
    "BKT": "Bankinter",
    "UNI": "Unicaja",
}


def upload_values(data, cur, yaml_data, date, parent_key=None):
    for key, value in yaml_data.items():
        if isinstance(value, dict):
            upload_values(data, cur, value, date, parent_key=key)
        else:
            entidad_code = key.split(".")[-1]
            entidad_nombre = CODES.get(entidad_code)
            cur.execute("SELECT id FROM metrics WHERE name = %s", (key,))
            metric_id = cur.fetchone()

            if metric_id and entidad_nombre:
                try:
                    raw_val = data.loc[(date, entidad_nombre), parent_key]

                    # --- NEW CLEANING LOGIC ---
                    if isinstance(raw_val, str):
                        clean_val = float(raw_val.replace(",", ".").replace("%", ""))
                        if "%" in raw_val:
                            clean_val = clean_val / 100
                    else:
                        clean_val = float(raw_val)
                    # --------------------------

                    if np.isnan(clean_val):
                        continue

                    cur.execute(
                        """
                        INSERT INTO values (date, value, metric_id, value_meta)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (metric_id, date) DO UPDATE SET value = EXCLUDED.value;
                    """,
                        (date, clean_val, metric_id[0], json.dumps({})),
                    )
                except (KeyError, ValueError, TypeError):
                    # Skip rows where data is missing or totally unparseable
                    pass


def main():
    if len(sys.argv) > 1:
        target_db = sys.argv[1]
    else:
        target_db = input(
            "Choose database to update (leave blank for default): "
        ).strip()
        if not target_db:
            target_db = None

    current_dir = Path(__file__).parent
    yaml_file = current_dir / "estructura_metricas_bancos.yaml"
    excel_file = (
        current_dir
        / "excels_raw_metricas_bancos"
        / "20231016 Cuadros Jose modificado.xlsx"
    )

    with open(yaml_file, "r", encoding="utf-8") as f:
        yaml_data = yaml.safe_load(f)

    print(f"📖 Reading Excel: {excel_file.name}")
    # Force engine openpyxl to handle newer .xlsx formats better
    df = pd.read_excel(excel_file, sheet_name="Agregado")
    df = df.set_index(["periodo", "entidad"])

    conn, cur = None, None
    try:
        conn, cur = get_db_connection(target_db=target_db)
        dates = df.index.get_level_values("periodo").unique()
        for d in dates:
            print(f"🚀 Processing date: {d}")
            upload_values(df, cur, yaml_data, d)
        conn.commit()
        print(f"✅ Data upload complete in '{target_db or 'default'}'.")
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
