"""
This script focuses on the structure. It ensures the tables exist and maps the "tree" (Categories -> Metrics -> Countries).
It reads from 'estructura_EBA.yaml' and populates the hierarchy and metrics tables in the PostgreSQL database.

in config > database.yaml choose
"""

import json
import yaml
import sys
from pathlib import Path
from typing import Optional, Dict, Any

# Setup paths
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from database.connection import get_db_connection


def create_eba_schema(cur):
    """Basic schema creation without forcing constraints."""
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS hierarchy (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            parent_id INTEGER REFERENCES hierarchy(id)
        );
    """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS metrics (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            dimensions JSONB,
            hierarchy_id INTEGER REFERENCES hierarchy(id)
        );
    """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS values (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            value DOUBLE PRECISION,
            metric_id INTEGER REFERENCES metrics(id),
            value_meta JSONB,
            UNIQUE(metric_id, date)
        );
    """
    )


def get_or_create_node(cur, name: str, parent_id: Optional[int]):
    """Checks for existing hierarchy node to avoid duplicates."""
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


def get_or_create_metric(cur, name: str, dimensions: dict, hierarchy_id: int):
    """
    Manual check for metric existence to avoid the ON CONFLICT error.
    """
    cur.execute("SELECT id FROM metrics WHERE name = %s;", (name,))
    row = cur.fetchone()

    if row:
        # Update existing record metadata
        cur.execute(
            "UPDATE metrics SET dimensions = %s, hierarchy_id = %s WHERE id = %s;",
            (json.dumps(dimensions), hierarchy_id, row[0]),
        )
        return row[0]
    else:
        # Insert new record
        cur.execute(
            "INSERT INTO metrics (name, dimensions, hierarchy_id) VALUES (%s, %s, %s) RETURNING id;",
            (name, json.dumps(dimensions), hierarchy_id),
        )
        return cur.fetchone()[0]


def sync_hierarchy(cur, data: Dict[str, Any], parent_id: Optional[int] = None):
    """Recursively processes the YAML structure."""
    for key, value in data.items():
        if isinstance(value, dict):
            node_id = get_or_create_node(cur, key, parent_id)
            sync_hierarchy(cur, value, node_id)
        else:
            # Replaced ON CONFLICT with manual function
            get_or_create_metric(cur, key, {"friendly_name": value}, parent_id)


def run_setup():
    if len(sys.argv) > 1:
        target_db = sys.argv[1]
    else:
        target_db = input(
            "Choose database to update (leave blank for default): "
        ).strip()
        if not target_db:
            target_db = None

    yaml_path = Path(__file__).parent / "estructura_EBA.yaml"
    conn, cur = None, None
    try:
        conn, cur = get_db_connection(target_db=target_db)
        create_eba_schema(cur)
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        sync_hierarchy(cur, data)
        conn.commit()
        print(
            f"✅ Metadata sync complete in '{target_db if target_db else 'default'}'."
        )
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"❌ Error during hierarchy sync: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    run_setup()
