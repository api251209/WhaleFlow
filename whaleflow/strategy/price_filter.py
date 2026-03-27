"""週漲幅過濾：排除近一週股價漲幅超過門檻的個股。"""

from whaleflow.strategy.tdcc_trend import StockSignal
from whaleflow.utils.logging import get_logger

logger = get_logger(__name__)


def filter_by_weekly_gain(
    signals: list[StockSignal],
    prices_t: dict[str, float],
    prices_t1: dict[str, float],
    max_gain: float = 0.20,
) -> list[StockSignal]:
    """
    Remove stocks whose weekly price gain exceeds max_gain.

    Args:
        signals:   Candidate stocks from TDCC trend scan.
        prices_t:  Closing prices for week T  (current week).
        prices_t1: Closing prices for week T-1 (previous week).
        max_gain:  Exclude if (close_T - close_T1) / close_T1 > max_gain.

    Returns:
        Filtered list (new list; input is not mutated).
        Stocks with missing price data are kept (conservative).
    """
    kept: list[StockSignal] = []
    excluded: list[tuple[str, float]] = []

    for sig in signals:
        p_t = prices_t.get(sig.stock_id)
        p_t1 = prices_t1.get(sig.stock_id)

        if p_t is None or p_t1 is None or p_t1 <= 0:
            kept.append(sig)
            continue

        gain = (p_t - p_t1) / p_t1
        if gain > max_gain:
            excluded.append((sig.stock_id, gain))
        else:
            kept.append(sig)

    if excluded:
        for stock_id, gain in excluded:
            logger.info(
                "Price filter: excluded %s (weekly gain %.1f%%)",
                stock_id, gain * 100,
            )

    return kept


def filter_by_liquidity(
    signals: list[StockSignal],
    volumes_by_week: list[dict[str, int]],
    min_avg_daily_volume: int = 1000,
) -> list[StockSignal]:
    """
    Remove stocks whose average daily volume is below the threshold.

    Args:
        signals:              Candidate stocks from TDCC trend scan.
        volumes_by_week:      List of {stock_id: volume_in_lots} dicts, one per week.
        min_avg_daily_volume: Minimum average daily volume in lots (張).
                              Stocks with missing data in all weeks are kept.

    Returns:
        Filtered list (new list; input is not mutated).
    """
    kept: list[StockSignal] = []
    for sig in signals:
        vols = [
            v_map[sig.stock_id]
            for v_map in volumes_by_week
            if sig.stock_id in v_map
        ]
        if not vols:
            kept.append(sig)  # keep if no volume data (conservative)
            continue
        avg = sum(vols) / len(vols)
        if avg >= min_avg_daily_volume:
            kept.append(sig)
        else:
            logger.info(
                "Liquidity filter: excluded %s (avg vol %.0f < %d)",
                sig.stock_id, avg, min_avg_daily_volume,
            )
    return kept
