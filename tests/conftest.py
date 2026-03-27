"""Shared pytest fixtures."""

import pytest
from sqlalchemy.orm import Session, sessionmaker

from whaleflow.db.engine import get_in_memory_engine
from whaleflow.db.models import Base


@pytest.fixture(scope="function")
def db_engine():
    engine = get_in_memory_engine()
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture(scope="function")
def db_session(db_engine) -> Session:
    factory = sessionmaker(bind=db_engine, expire_on_commit=False)
    session = factory()
    yield session
    session.rollback()
    session.close()
