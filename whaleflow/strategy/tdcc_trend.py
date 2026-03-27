"""TDCC 集保分佈趨勢分析與選股評分。

Three independent conditions (satisfy ANY ONE to qualify):

  A. pct_above_400  increases in ALL 3 intervals across 4 weeks (T-3→T-2→T-1→T)
  B. shares_above_800 increases in BOTH intervals across 3 weeks (T-2→T-1→T)
  C. holders_above_1000 AND shares_above_1000 BOTH increase (T-1→T)

Noise filter: a holder-count delta of ≤ 2 (|delta| ≤ 2) is treated as noise
and invalidates the condition for that interval.
"""

from dataclasses import dataclass
from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from whaleflow.db.models import TdccDistribution
from whaleflow.utils.logging import get_logger

logger = get_logger(__name__)

# ── 參數（預設值，可由外部傳入覆蓋） ────────────────────────────────────────────

MIN_HOLDER_DELTA = 3   # |delta_holders| must exceed this to be a real signal
_CONDITION_A_ENABLED = True
_CONDITION_B_ENABLED = True
_CONDITION_C_ENABLED = True


# ── 資料結構 ──────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class WeekSnapshot:
    stock_id: str
    week_date: date
    holders_400: int
    shares_400: int
    pct_400: float
    holders_800: int
    shares_800: int
    pct_800: float
    holders_1000: int
    shares_1000: int
    pct_1000: float


@dataclass(frozen=True)
class StockSignal:
    stock_id: str
    week_date: date          # latest week (T)
    score: float
    conditions: tuple[str, ...]   # subset of ("A", "B", "C") that passed
    detail: str              # human-readable summary


# ── 噪音過濾 ──────────────────────────────────────────────────────────────────

def _valid_holder_increase(before: int, after: int) -> bool:
    """Return True if holder count increased by more than MIN_HOLDER_DELTA."""
    return (after - before) > MIN_HOLDER_DELTA


def _valid_holder_delta(before: int, after: int) -> bool:
    """Return True if holder count delta is meaningful (|delta| > MIN_HOLDER_DELTA)."""
    return abs(after - before) > MIN_HOLDER_DELTA


# ── 三個選股條件 ───────────────────────────────────────────────────────────────

def _check_A(
    snaps: list[WeekSnapshot],
    min_pct_delta: float = 0.0,
    min_holder_delta_a: int | None = None,
    lookback_weeks: int = 4,
) -> tuple[bool, str]:
    """
    Condition A: pct_above_400 at T is higher than at T-(lookback_weeks-1).

    Args:
        min_pct_delta: T pct must exceed T-N pct by at least this amount.
        min_holder_delta_a: holders_400 at T must exceed T-N by at least this amount.
                            Defaults to MIN_HOLDER_DELTA if None.
        lookback_weeks: Total weeks to look back (default 4 = compare T vs T-3).
    """
    if len(snaps) < lookback_weeks:
        return False, ""
    t_old, t0 = snaps[-lookback_weeks], snaps[-1]

    holder_threshold = min_holder_delta_a if min_holder_delta_a is not None else MIN_HOLDER_DELTA
    holder_delta = t0.holders_400 - t_old.holders_400
    if holder_delta < holder_threshold:
        return False, ""

    pct_delta = t0.pct_400 - t_old.pct_400
    if pct_delta <= min_pct_delta:
        return False, ""

    n = lookback_weeks - 1
    detail = (
        f"A: pct400 {t_old.pct_400:.2f}%→{t0.pct_400:.2f}% ({pct_delta:+.2f}%) "
        f"人數{t_old.holders_400}→{t0.holders_400} ({holder_delta:+d}人) ({lookback_weeks}週淨增)"
    )
    return True, detail


def _check_B(
    snaps: list[WeekSnapshot],
    min_holder_delta_b: int | None = None,
    consecutive_weeks: int = 2,
    min_share_delta: int = 0,
) -> tuple[bool, str]:
    """
    Condition B: shares_above_800 increases in all consecutive intervals.

    Args:
        min_holder_delta_b: holders_800 delta per interval must exceed this.
                            Defaults to MIN_HOLDER_DELTA if None.
        consecutive_weeks: Number of consecutive weekly increases required (default 2).
                           Requires (consecutive_weeks + 1) snapshots.
        min_share_delta: shares_800 increase per interval must exceed this (in lots).
    """
    needed = consecutive_weeks + 1
    if len(snaps) < needed:
        return False, ""
    threshold = min_holder_delta_b if min_holder_delta_b is not None else MIN_HOLDER_DELTA
    window = snaps[-needed:]
    pairs = [(window[i], window[i + 1]) for i in range(len(window) - 1)]
    deltas = []
    for before, after in pairs:
        if abs(after.holders_800 - before.holders_800) <= threshold:
            return False, ""
        share_delta = after.shares_800 - before.shares_800
        if share_delta <= min_share_delta:
            return False, ""
        deltas.append(share_delta)
    delta_strs = "/".join(
        f"+{d//1000}k" if d >= 1000 else f"+{d}" for d in deltas
    )
    detail = f"B: shares800 {delta_strs} ({consecutive_weeks}週)"
    return True, detail


def _check_C(
    snaps: list[WeekSnapshot],
    min_holder_delta_c: int | None = None,
    min_share_delta_c: int = 0,
) -> tuple[bool, str]:
    """
    Condition C: BOTH holders_above_1000 AND shares_above_1000 increase (T-1 → T).

    Args:
        min_holder_delta_c: holders_1000 increase must exceed this.
                            Defaults to MIN_HOLDER_DELTA if None.
        min_share_delta_c: shares_1000 increase must exceed this (in lots).
    """
    if len(snaps) < 2:
        return False, ""
    t1, t0 = snaps[-2], snaps[-1]
    dh = t0.holders_1000 - t1.holders_1000
    ds = t0.shares_1000 - t1.shares_1000
    threshold = min_holder_delta_c if min_holder_delta_c is not None else MIN_HOLDER_DELTA
    if dh <= threshold:
        return False, ""
    if ds <= min_share_delta_c:
        return False, ""
    ds_str = f"+{ds//1000}k" if ds >= 1000 else f"+{ds}"
    detail = f"C: h1000+{dh} shares1000{ds_str}"
    return True, detail


# ── 主評分函式 ────────────────────────────────────────────────────────────────

_CONDITION_SCORES = {"A": 3.0, "B": 2.0, "C": 2.5}


def score_stock(
    snaps: list[WeekSnapshot],
    min_holder_delta: int = MIN_HOLDER_DELTA,
    scores: dict[str, float] | None = None,
    enabled: dict[str, bool] | None = None,
) -> StockSignal | None:
    """
    Evaluate all three conditions for a stock.

    Args:
        snaps: Weekly snapshots ordered oldest first.
        min_holder_delta: Noise threshold for holder count changes.
        scores: Override condition scores e.g. {"A": 3.0, "B": 2.0, "C": 2.5}.
        enabled: Which conditions to check e.g. {"A": True, "B": False, "C": True}.

    Returns None if no condition is satisfied.
    """
    import whaleflow.strategy.tdcc_trend as _self
    _self.MIN_HOLDER_DELTA = min_holder_delta

    condition_scores = {**_CONDITION_SCORES, **(scores or {})}
    condition_enabled = {"A": True, "B": True, "C": True}
    if enabled:
        condition_enabled.update(enabled)

    cfg_a = (scores or {}).get("_a_cfg", {})
    cfg_b = (scores or {}).get("_b_cfg", {})
    cfg_c = (scores or {}).get("_c_cfg", {})
    results: list[tuple[str, str]] = []
    for label, check_fn in [("A", _check_A), ("B", _check_B), ("C", _check_C)]:
        if not condition_enabled.get(label, True):
            continue
        if label == "A":
            passed, detail = check_fn(
                snaps,
                min_pct_delta=cfg_a.get("min_pct_delta", 0.0),
                min_holder_delta_a=cfg_a.get("min_holder_delta", min_holder_delta),
                lookback_weeks=cfg_a.get("lookback_weeks", 4),
            )
        elif label == "B":
            passed, detail = check_fn(
                snaps,
                min_holder_delta_b=cfg_b.get("min_holder_delta", min_holder_delta),
                consecutive_weeks=cfg_b.get("consecutive_weeks", 2),
                min_share_delta=cfg_b.get("min_share_delta", 0),
            )
        else:
            passed, detail = check_fn(
                snaps,
                min_holder_delta_c=cfg_c.get("min_holder_delta", min_holder_delta),
                min_share_delta_c=cfg_c.get("min_share_delta", 0),
            )
        if passed:
            results.append((label, detail))

    if not results:
        return None

    conditions = tuple(label for label, _ in results)
    score = sum(condition_scores[c] for c in conditions)
    detail = " | ".join(d for _, d in results)

    return StockSignal(
        stock_id=snaps[-1].stock_id,
        week_date=snaps[-1].week_date,
        score=round(score, 2),
        conditions=conditions,
        detail=detail,
    )


# ── DB 查詢 ───────────────────────────────────────────────────────────────────

def _load_snapshots(
    session: Session,
    week_dates: list[date],
) -> dict[str, list[WeekSnapshot]]:
    """
    Load TdccDistribution rows for the given weeks.

    Returns:
        {stock_id: [WeekSnapshot, ...]} ordered by week_date ascending.
    """
    stmt = (
        select(TdccDistribution)
        .where(TdccDistribution.week_date.in_(week_dates))
        .order_by(TdccDistribution.week_date)
    )
    rows = session.execute(stmt).scalars().all()

    result: dict[str, list[WeekSnapshot]] = {}
    for row in rows:
        snap = WeekSnapshot(
            stock_id=row.stock_id,
            week_date=row.week_date,
            holders_400=row.holders_above_400 or 0,
            shares_400=row.shares_above_400 or 0,
            pct_400=row.pct_above_400 or 0.0,
            holders_800=row.holders_above_800 or 0,
            shares_800=row.shares_above_800 or 0,
            pct_800=row.pct_above_800 or 0.0,
            holders_1000=row.holders_above_1000 or 0,
            shares_1000=row.shares_above_1000 or 0,
            pct_1000=row.pct_above_1000 or 0.0,
        )
        result.setdefault(row.stock_id, []).append(snap)

    return result


def scan_weekly(
    session: Session,
    weeks: list[date],
    min_holder_delta: int = MIN_HOLDER_DELTA,
    scores: dict[str, float] | None = None,
    enabled: dict[str, bool] | None = None,
) -> list[StockSignal]:
    """
    Run the full TDCC trend scan.

    Args:
        session: DB session.
        weeks: Sorted list of week dates to include (oldest first).
                Condition A requires ≥ 4 weeks; B requires ≥ 3; C requires ≥ 2.
        min_holder_delta: Noise threshold (from config).
        scores: Condition score overrides (from config).
        enabled: Which conditions to evaluate (from config).

    Returns:
        List of StockSignal sorted by score descending.
    """
    snapshots_by_stock = _load_snapshots(session, weeks)

    logger.info(
        "Scanning %d stocks across %d weeks (%s → %s)",
        len(snapshots_by_stock), len(weeks), weeks[0], weeks[-1],
    )

    signals: list[StockSignal] = []
    for stock_id, snaps in snapshots_by_stock.items():
        if len(snaps) < 2:
            continue
        signal = score_stock(snaps, min_holder_delta=min_holder_delta,
                             scores=scores, enabled=enabled)
        if signal:
            signals.append(signal)

    signals.sort(key=lambda s: s.score, reverse=True)
    logger.info("Found %d candidate stocks", len(signals))
    return signals
