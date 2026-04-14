"""initial_schema

Captures the existing database schema as a baseline migration.
Tables: hierarchy, metrics, values, demo_series.

Revision ID: 0001
Revises: None
Create Date: 2026-04-14
"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # -- hierarchy: tree structure / folders --
    op.execute("""
        CREATE TABLE IF NOT EXISTS hierarchy (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            parent_id INTEGER REFERENCES hierarchy(id)
        );
    """)

    # -- metrics: specific series (e.g. ROE for Spain) --
    op.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            dimensions JSONB,
            hierarchy_id INTEGER REFERENCES hierarchy(id)
        );
    """)

    # -- values: actual data points for each metric --
    op.execute("""
        CREATE TABLE IF NOT EXISTS values (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            value DOUBLE PRECISION,
            metric_id INTEGER REFERENCES metrics(id),
            value_meta JSONB,
            UNIQUE(metric_id, date)
        );
    """)

    # -- demo_series: demo/CRUD learning table --
    op.execute("""
        CREATE TABLE IF NOT EXISTS demo_series (
            id SERIAL PRIMARY KEY,
            title VARCHAR(100) NOT NULL UNIQUE,
            genre VARCHAR(50),
            seasons INTEGER,
            rating NUMERIC(3, 1),
            release_year INTEGER,
            streaming_platform VARCHAR(30)
        );
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS demo_series;")
    op.execute("DROP TABLE IF EXISTS values;")
    op.execute("DROP TABLE IF EXISTS metrics;")
    op.execute("DROP TABLE IF EXISTS hierarchy;")
