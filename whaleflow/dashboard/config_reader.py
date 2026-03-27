"""從 scan_config.toml 讀取並解析掃描參數。

提取自 __main__.py 的設定解析邏輯，供 CLI 與 Dashboard 共用。
"""

import tomllib
from dataclasses import dataclass, field
from pathlib import Path

# scan_config.toml 位置（專案根目錄）
_CONFIG_PATH = Path(__file__).parents[2] / "scan_config.toml"


@dataclass
class ScanConfig:
    """所有掃描參數的結構化表示。"""

    # 雜訊門檻
    min_holder_delta: int = 3

    # 條件啟用
    enabled: dict = field(default_factory=lambda: {"A": True, "B": True, "C": True})

    # 條件分數與子參數（含 _a_cfg / _b_cfg / _c_cfg）
    scores: dict = field(default_factory=dict)

    # 總分門檻
    min_total_score: float = 0.0

    # 週漲幅過濾
    max_weekly_gain: float = 0.20
    price_filter_enabled: bool = True

    # 流動性過濾
    min_avg_daily_volume: int = 1000
    liquidity_filter_enabled: bool = False

    def __post_init__(self) -> None:
        if not self.scores:
            self.scores = _default_scores(self.min_holder_delta)


def _default_scores(min_holder_delta: int) -> dict:
    return {
        "A": 3.0, "B": 2.0, "C": 2.5,
        "_a_cfg": {"min_pct_delta": 0.0, "min_holder_delta": min_holder_delta, "lookback_weeks": 4},
        "_b_cfg": {"min_holder_delta": min_holder_delta, "consecutive_weeks": 2, "min_share_delta": 0},
        "_c_cfg": {"min_holder_delta": min_holder_delta, "min_share_delta": 0},
    }


def load_scan_config(config_path: Path | None = None) -> ScanConfig:
    """讀取 scan_config.toml，回傳 ScanConfig。找不到設定檔時使用預設值。"""
    path = config_path or _CONFIG_PATH
    if not path.exists():
        return ScanConfig()

    with path.open("rb") as f:
        cfg = tomllib.load(f)

    noise_cfg   = cfg.get("noise", {})
    cond_cfg    = cfg.get("conditions", {})
    price_cfg   = cfg.get("price_filter", {})
    scoring_cfg = cfg.get("scoring", {})
    liq_cfg     = cfg.get("liquidity_filter", {})

    min_holder_delta = int(noise_cfg.get("min_holder_delta", 3))

    scores = {
        "A": float(cond_cfg.get("condition_a_score", 3.0)),
        "B": float(cond_cfg.get("condition_b_score", 2.0)),
        "C": float(cond_cfg.get("condition_c_score", 2.5)),
        "_a_cfg": {
            "min_pct_delta":   float(cond_cfg.get("condition_a_min_pct_delta", 0.0)),
            "min_holder_delta": int(cond_cfg.get("condition_a_min_holder_delta", min_holder_delta)),
            "lookback_weeks":   int(cond_cfg.get("condition_a_lookback_weeks", 4)),
        },
        "_b_cfg": {
            "min_holder_delta": int(cond_cfg.get("condition_b_min_holder_delta", min_holder_delta)),
            "consecutive_weeks": int(cond_cfg.get("condition_b_consecutive_weeks", 2)),
            "min_share_delta":   int(cond_cfg.get("condition_b_min_share_delta", 0)),
        },
        "_c_cfg": {
            "min_holder_delta": int(cond_cfg.get("condition_c_min_holder_delta", min_holder_delta)),
            "min_share_delta":  int(cond_cfg.get("condition_c_min_share_delta", 0)),
        },
    }

    enabled = {
        "A": bool(cond_cfg.get("condition_a_enabled", True)),
        "B": bool(cond_cfg.get("condition_b_enabled", True)),
        "C": bool(cond_cfg.get("condition_c_enabled", True)),
    }

    return ScanConfig(
        min_holder_delta=min_holder_delta,
        enabled=enabled,
        scores=scores,
        min_total_score=float(scoring_cfg.get("min_total_score", 0.0)),
        max_weekly_gain=float(price_cfg.get("max_weekly_gain", 0.20)),
        price_filter_enabled=bool(price_cfg.get("enabled", True)),
        min_avg_daily_volume=int(liq_cfg.get("min_avg_daily_volume", 1000)),
        liquidity_filter_enabled=bool(liq_cfg.get("enabled", False)),
    )
