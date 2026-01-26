# Database connection configuration file & data logic

import os
import yaml
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

# 1. Get Absolute Paths
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"
CONFIG_PATH = BASE_DIR / "config" / "database.yaml"

# 2. Load .env if it exists
if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH, override=True)
else:
    print(f".env file not found at {ENV_PATH}")


# 3. Load DB config with optional target_db override
def load_db_config(target_db=None):
    """
    Loads configuration. If target_db is provided, it overrides
    the 'database' field from the YAML file.
    """
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)

    db_pass = os.getenv("DB_PASSWORD")
    if db_pass:
        db_pass = db_pass.strip()
    else:
        print(f"❌ ERROR: DB_PASSWORD is empty in {ENV_PATH}")

    # Use target_db if provided, else use YAML default
    selected_db = target_db if target_db else config["database"]

    return {
        "dbname": selected_db,
        "user": config["username"],
        "password": db_pass,
        "host": config["host"],
        "port": config.get("port", 5432),
    }


# 4. Explicit connection helper with optional override
def get_db_connection(target_db=None):
    """
    Returns a connection and cursor.
    Accepts an optional database name to switch target on the fly.
    """
    params = load_db_config(target_db)
    print(f"🔌 Connecting to database: {params['dbname']}")

    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    return conn, cur
