import json
import yaml
import sys
from pathlib import Path
from typing import Optional, Dict, Any

# Step back 3 levels: metricas_bancos -> upload_excel_to_database -> root
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from database.connection import get_db_connection


def get_or_create_hierarchy(cur, name: str, parent_id: Optional[int]):
    cur.execute(
        "SELECT id FROM hierarchy WHERE name = %s AND parent_id IS NOT DISTINCT FROM %s;",
        (name, parent_id),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO hierarchy (name, parent_id) VALUES (%s, %s) RETURNING id;",
        (name, parent_id),
    )
    return cur.fetchone()[0]


def process_hierarchy(cur, data: Dict[str, Any], parent_id: Optional[int] = None):
    for key, value in data.items():
        if isinstance(value, dict):
            node_id = get_or_create_hierarchy(cur, key, parent_id)
            process_hierarchy(cur, value, node_id)
        else:
            dimensions = {"name": value}
            cur.execute("SELECT id FROM metrics WHERE name = %s;", (key,))
            row = cur.fetchone()
            if row:
                cur.execute(
                    "UPDATE metrics SET hierarchy_id = %s, dimensions = %s WHERE id = %s;",
                    (parent_id, json.dumps(dimensions), row[0]),
                )
            else:
                cur.execute(
                    "INSERT INTO metrics (name, dimensions, hierarchy_id) VALUES (%s, %s, %s);",
                    (key, json.dumps(dimensions), parent_id),
                )


def run_setup():
    # INTERACTIVE MODE
    if len(sys.argv) > 1:
        target_db = sys.argv[1]
    else:
        target_db = input(
            "Choose database to update (leave blank for default): "
        ).strip()
        if not target_db:
            target_db = None

    yaml_path = Path(__file__).parent / "estructura_metricas_bancos.yaml"
    conn, cur = None, None
    try:
        conn, cur = get_db_connection(target_db=target_db)
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        process_hierarchy(cur, data)
        conn.commit()
        print(f"✅ Bank hierarchy sync complete in '{target_db or 'default'}'.")
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
    run_setup()
