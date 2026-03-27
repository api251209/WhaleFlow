"""
End-to-end tests that call the real FinMind API.
Skipped in CI by default. Run manually with:
    pytest tests/e2e/ -m e2e -v
"""

import asyncio

import pytest

from whaleflow.config import settings
from whaleflow.fetchers.finmind import FinMindFetcher
from whaleflow.utils.date_utils import get_last_friday, date_to_str


pytestmark = pytest.mark.e2e


@pytest.mark.skipif(not settings.finmind_api_token, reason="No FINMIND_API_TOKEN set")
def test_fetch_stock_info_live():
    fetcher = FinMindFetcher()
    rows = asyncio.run(fetcher.fetch("TaiwanStockInfo", {}))
    assert len(rows) > 100, "Expected >100 stocks from TaiwanStockInfo"
    sample = rows[0]
    assert "stock_id" in sample


@pytest.mark.skipif(not settings.finmind_api_token, reason="No FINMIND_API_TOKEN set")
def test_fetch_tdcc_by_date_live():
    """
    CRITICAL: Verify FinMind supports date-only query (no stock_id).
    If this returns data, our strategy A (single call per week) works.
    If empty, we must fall back to per-stock queries (strategy B).
    """
    fetcher = FinMindFetcher()
    friday = get_last_friday()
    rows = asyncio.run(
        fetcher.fetch("TaiwanStockShareholding", {"date": date_to_str(friday)})
    )

    print(f"\nTDCC date-only query returned {len(rows)} rows for {friday}")
    if rows:
        print(f"Sample row: {rows[0]}")
        assert "stock_id" in rows[0]
        assert "HoldingSharesLevel" in rows[0]
    else:
        pytest.skip(
            "date-only TDCC query returned 0 rows -- "
            "Strategy A not supported, need to implement Strategy B (per-stock)"
        )
