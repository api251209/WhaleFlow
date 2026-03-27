"""
TDCC website scraper for 股東持股分級表 (集保分佈).

Strategy:
  1. GET the query page once to obtain SYNCHRONIZER_TOKEN + session cookies.
  2. Reuse the same session + token for all subsequent per-stock POST requests.
  3. Token is valid for the entire session; refresh when a redirect back to the
     form page is detected (token expired).

TDCC URL: https://www.tdcc.com.tw/portal/zh/smWeb/qryStock
Update frequency: weekly (every Friday settlement)
"""

import asyncio
import re
from datetime import date

import httpx

from whaleflow.utils.date_utils import date_to_str
from whaleflow.utils.logging import get_logger

logger = get_logger(__name__)

_BASE_URL = "https://www.tdcc.com.tw/portal/zh/smWeb/qryStock"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": _BASE_URL,
    "Content-Type": "application/x-www-form-urlencoded",
}

# Maps TDCC HTML bracket labels → our internal level key
_BRACKET_MAP = {
    "400,001-600,000": "400",
    "600,001-800,000": "600",
    "800,001-1,000,000": "800",
    "1,000,001以上": "1000",
}


def _parse_int(s: str) -> int:
    return int(s.replace(",", "").strip()) if s.strip() else 0


def _parse_float(s: str) -> float:
    return float(s.replace(",", "").strip()) if s.strip() else 0.0


def _parse_table(html: str) -> list[dict] | None:
    """
    Extract bracket rows from TDCC result HTML.

    Returns None if no data found ('查無此資料'), otherwise a list of dicts:
        [{"level": "400,001-600,000", "persons": int, "shares": int, "pct": float}, ...]
    """
    tables = re.findall(r"<table[^>]*>.*?</table>", html, re.DOTALL)
    if len(tables) < 2:
        return None

    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", tables[1], re.DOTALL)
    results: list[dict] = []
    total_row: dict | None = None

    for row in rows:
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.DOTALL)
        clean = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]

        if not clean or len(clean) < 2:
            continue
        if clean[0] == "序":  # header row
            continue
        if "查無此資料" in clean[0]:
            return None  # no data for this stock/date

        if len(clean) >= 5:
            level = clean[1]
            # Total row
            if "合" in level:
                total_row = {
                    "level": "合計",
                    "persons": _parse_int(clean[2]),
                    "shares": _parse_int(clean[3]),
                    "pct": _parse_float(clean[4]),
                }
                continue
            results.append(
                {
                    "level": level,
                    "persons": _parse_int(clean[2]),
                    "shares": _parse_int(clean[3]),
                    "pct": _parse_float(clean[4]),
                }
            )

    if total_row:
        results.append(total_row)
    return results if results else None


def _accumulate(rows: list[dict]) -> dict:
    """
    From raw bracket rows, compute cumulative holders/shares/pct for
    400 / 600 / 800 / 1000 張以上, plus totals.
    """
    level_map = {r["level"]: r for r in rows}
    total = level_map.get("合計", {})

    result: dict = {
        "total_holders": total.get("persons", 0),
        "total_shares": total.get("shares", 0),
    }

    cumulative_labels = {
        "400": ["400,001-600,000", "600,001-800,000", "800,001-1,000,000", "1,000,001以上"],
        "600": ["600,001-800,000", "800,001-1,000,000", "1,000,001以上"],
        "800": ["800,001-1,000,000", "1,000,001以上"],
        "1000": ["1,000,001以上"],
    }

    for threshold, labels in cumulative_labels.items():
        h = sum(level_map.get(lbl, {}).get("persons", 0) for lbl in labels)
        s = sum(level_map.get(lbl, {}).get("shares", 0) for lbl in labels)
        p = sum(level_map.get(lbl, {}).get("pct", 0.0) for lbl in labels)
        result[f"holders_above_{threshold}"] = h
        result[f"shares_above_{threshold}"] = s
        result[f"pct_above_{threshold}"] = round(p, 4)

    return result


class TdccScraper:
    """
    Session-aware TDCC scraper. Creates one HTTP session per run and
    refreshes the SYNCHRONIZER_TOKEN automatically when needed.
    """

    def __init__(self, delay: float = 1.0):
        """
        Args:
            delay: Seconds to wait between requests (be polite to TDCC).
        """
        self._delay = delay
        self._client: httpx.AsyncClient | None = None
        self._token: str = ""

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers=_HEADERS,
        )
        await self._refresh_token()
        return self

    async def __aexit__(self, *_):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _refresh_token(self) -> None:
        assert self._client
        r = await self._client.get(_BASE_URL)
        r.raise_for_status()
        m = re.search(r'name="SYNCHRONIZER_TOKEN"\s+value="([^"]+)"', r.text)
        if not m:
            raise RuntimeError("TDCC: SYNCHRONIZER_TOKEN not found in page")
        self._token = m.group(1)
        logger.debug("TDCC token refreshed: %s...", self._token[:8])

    def _get_available_dates(self, html: str) -> list[str]:
        """Extract available week dates from the <select> dropdown."""
        return re.findall(r'<option value="(\d{8})"', html)

    async def fetch_stock(
        self,
        stock_id: str,
        week_date: date,
    ) -> dict | None:
        """
        Fetch TDCC distribution for one stock on one week date.

        Returns:
            Accumulated dict with holders/shares/pct for each threshold,
            or None if no data.
        """
        assert self._client, "Use as async context manager"
        date_str = week_date.strftime("%Y%m%d")

        r = await self._client.post(
            _BASE_URL,
            data={
                "SYNCHRONIZER_TOKEN": self._token,
                "SYNCHRONIZER_URI": "/portal/zh/smWeb/qryStock",
                "method": "submit",
                "firDate": date_str,
                "scaDate": date_str,
                "sqlMethod": "StockNo",
                "stockNo": stock_id,
                "stockName": "",
            },
        )

        # TDCC rotates the token on every response — update it for the next request.
        m = re.search(r'name="SYNCHRONIZER_TOKEN"\s+value="([^"]+)"', r.text)
        if m:
            self._token = m.group(1)

        # Session fully expired (redirect to login page)
        if "請重新" in r.text:
            logger.warning("TDCC session expired for %s, refreshing...", stock_id)
            await self._refresh_token()
            return await self.fetch_stock(stock_id, week_date)

        rows = _parse_table(r.text)
        if rows is None:
            return None

        accumulated = _accumulate(rows)
        accumulated["stock_id"] = stock_id
        return accumulated

    async def get_available_dates(self) -> list[str]:
        """Return list of available TDCC dates (YYYYMMDD strings)."""
        assert self._client
        r = await self._client.get(_BASE_URL)
        return self._get_available_dates(r.text)

    async def fetch_all_stocks(
        self,
        stock_ids: list[str],
        week_date: date,
        on_progress=None,
    ) -> list[dict]:
        """
        Fetch TDCC distribution for all given stocks on week_date.

        Args:
            stock_ids: List of stock IDs to scrape.
            week_date: The Friday settlement date.
            on_progress: Optional callback(done, total) for progress reporting.

        Returns:
            List of accumulated dicts (only stocks with data).
        """
        results: list[dict] = []
        total = len(stock_ids)

        for i, stock_id in enumerate(stock_ids, 1):
            try:
                data = await self.fetch_stock(stock_id, week_date)
                if data:
                    results.append(data)
            except Exception as e:
                logger.warning("TDCC fetch failed for %s: %s", stock_id, e)

            if on_progress:
                on_progress(i, total)

            await asyncio.sleep(self._delay)

        return results
