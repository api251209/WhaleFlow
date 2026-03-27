"""Integration tests for tdcc_service using in-memory DB and mock scraper."""

import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from whaleflow.db.models import Stock, TdccDistribution
from whaleflow.services.stock_service import sync_stock_list
from whaleflow.services.tdcc_service import fetch_and_store_tdcc
from whaleflow.fetchers.finmind import FinMindFetcher

MOCK_STOCK_ROWS = [
    {"stock_id": "2330", "stock_name": "台積電", "type": "twse", "industry_category": "半導體"},
    {"stock_id": "2317", "stock_name": "鴻海", "type": "twse", "industry_category": "電子零組件"},
]

# What TdccScraper.fetch_stock returns for each stock
MOCK_SCRAPER_DATA = {
    "2330": {
        "stock_id": "2330",
        "holders_above_400": 55,
        "shares_above_400": 75_000_000,
        "pct_above_400": 18.75,
        "holders_above_600": 20,
        "shares_above_600": 50_000_000,
        "pct_above_600": 12.50,
        "holders_above_800": 10,
        "shares_above_800": 30_000_000,
        "pct_above_800": 10.00,
        "holders_above_1000": 5,
        "shares_above_1000": 20_000_000,
        "pct_above_1000": 8.00,
        "total_holders": 100_000,
        "total_shares": 400_000_000,
    },
    "2317": {
        "stock_id": "2317",
        "holders_above_400": 30,
        "shares_above_400": 15_000_000,
        "pct_above_400": 4.0,
        "holders_above_600": 0,
        "shares_above_600": 0,
        "pct_above_600": 0.0,
        "holders_above_800": 0,
        "shares_above_800": 0,
        "pct_above_800": 0.0,
        "holders_above_1000": 0,
        "shares_above_1000": 0,
        "pct_above_1000": 0.0,
        "total_holders": 80_000,
        "total_shares": 375_000_000,
    },
}


@pytest.fixture
def mock_finmind_stocks():
    fetcher = AsyncMock(spec=FinMindFetcher)
    fetcher.fetch = AsyncMock(return_value=MOCK_STOCK_ROWS)
    return fetcher


def _make_mock_scraper(data_map: dict):
    """Return an async context manager mock for TdccScraper."""
    scraper = MagicMock()
    scraper.__aenter__ = AsyncMock(return_value=scraper)
    scraper.__aexit__ = AsyncMock(return_value=None)
    scraper.fetch_stock = AsyncMock(side_effect=lambda stock_id, week_date: data_map.get(stock_id))
    return scraper


def test_sync_stock_list_upserts(db_session, mock_finmind_stocks):
    count = asyncio.run(sync_stock_list(db_session, mock_finmind_stocks))
    db_session.commit()
    assert count == 2
    stock = db_session.get(Stock, "2330")
    assert stock is not None
    assert stock.stock_name == "台積電"
    assert stock.market == "TWSE"
    assert stock.is_etf is False


def test_sync_stock_list_idempotent(db_session, mock_finmind_stocks):
    asyncio.run(sync_stock_list(db_session, mock_finmind_stocks))
    db_session.commit()
    count2 = asyncio.run(sync_stock_list(db_session, mock_finmind_stocks))
    db_session.commit()
    assert count2 == 2


def test_fetch_and_store_tdcc(db_session, mock_finmind_stocks):
    # Seed stocks
    asyncio.run(sync_stock_list(db_session, mock_finmind_stocks))
    db_session.commit()

    mock_scraper = _make_mock_scraper(MOCK_SCRAPER_DATA)
    with patch("whaleflow.services.tdcc_service.TdccScraper", return_value=mock_scraper):
        count = asyncio.run(
            fetch_and_store_tdcc(db_session, date(2026, 3, 20))
        )
    db_session.commit()

    assert count == 2
    rec = db_session.get(TdccDistribution, ("2330", date(2026, 3, 20)))
    assert rec is not None
    assert rec.holders_above_400 == 55
    assert rec.holders_above_1000 == 5
    assert abs(rec.pct_above_400 - 18.75) < 0.01


def test_fetch_tdcc_empty_response(db_session, mock_finmind_stocks):
    asyncio.run(sync_stock_list(db_session, mock_finmind_stocks))
    db_session.commit()

    mock_scraper = _make_mock_scraper({})  # all stocks return None
    with patch("whaleflow.services.tdcc_service.TdccScraper", return_value=mock_scraper):
        count = asyncio.run(fetch_and_store_tdcc(db_session, date(2026, 3, 20)))
    assert count == 0
