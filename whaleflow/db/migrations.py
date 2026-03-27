from sqlalchemy import text

from whaleflow.db.engine import get_engine
from whaleflow.db.models import Base, StrategyConfig
from whaleflow.utils.logging import get_logger

logger = get_logger(__name__)


def _add_column_if_missing(engine, table: str, column: str, col_type: str) -> None:
    """SQLite-compatible: add column to table only if it doesn't exist."""
    with engine.connect() as conn:
        rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
        existing = [r[1] for r in rows]
        if column not in existing:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"))
            conn.commit()
            logger.info("Migration: added column %s.%s", table, column)


def init_db() -> None:
    """Create all tables, run column migrations, and seed default strategy config."""
    engine = get_engine()
    Base.metadata.create_all(engine)
    logger.info("Database schema created.")

    # Column migrations for tables that may already exist
    _add_column_if_missing(engine, "weekly_price", "volume", "BIGINT")

    # Seed default strategy config (id=1 only)
    from whaleflow.db.engine import get_session

    with get_session() as session:
        existing = session.get(StrategyConfig, 1)
        if existing is None:
            session.add(StrategyConfig(id=1))
            logger.info("Default strategy config seeded.")
