"""
Alembic environment configuration.

Builds the database URL from the same .env + config/database.yaml
that the rest of the project already uses, so there is no need to
duplicate connection details.
"""

import os
import yaml
from pathlib import Path
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool
from dotenv import load_dotenv

# ── Alembic Config object (reads alembic.ini) ──────────────────────
config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# ── Build the DB URL from existing project config ──────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"
CONFIG_PATH = BASE_DIR / "config" / "database.yaml"

if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH, override=True)

with open(CONFIG_PATH, "r") as f:
    db_config = yaml.safe_load(f)

db_password = os.getenv("DB_PASSWORD", "")
db_url = (
    f"postgresql://{db_config['username']}:{db_password}"
    f"@{db_config['host']}:{db_config.get('port', 5432)}"
    f"/{db_config['database']}"
)

# Override the placeholder URL from alembic.ini
config.set_main_option("sqlalchemy.url", db_url)

# ── No SQLAlchemy models (we use raw SQL in migrations) ────────────
target_metadata = None


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode — emits SQL without connecting."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode — connects and executes."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
