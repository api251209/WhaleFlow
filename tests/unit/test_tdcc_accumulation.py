"""Test the TDCC bracket accumulation logic (now in tdcc_scraper)."""

from whaleflow.fetchers.tdcc_scraper import _accumulate, _parse_table

# Build rows via _parse_table to test the full pipeline
_HTML = """
<table><tr><td>form</td></tr></table>
<table>
  <tr><th>序</th><th>持股/單位數分級</th><th>人數</th><th>股數/單位數</th><th>占集保庫存數比例 (%)</th></tr>
  <tr><td>1</td><td>1-999</td><td>100,000</td><td>50,000,000</td><td>0.50</td></tr>
  <tr><td>12</td><td>400,001-600,000</td><td>50</td><td>25,000,000</td><td>6.25</td></tr>
  <tr><td>13</td><td>600,001-800,000</td><td>20</td><td>14,000,000</td><td>3.50</td></tr>
  <tr><td>14</td><td>800,001-1,000,000</td><td>10</td><td>9,000,000</td><td>2.25</td></tr>
  <tr><td>15</td><td>1,000,001以上</td><td>5</td><td>50,000,000</td><td>12.50</td></tr>
  <tr><td>16</td><td>合　計</td><td>100,085</td><td>148,000,000</td><td>100.00</td></tr>
</table>
"""


def _acc():
    rows = _parse_table(_HTML)
    return _accumulate(rows)


def test_accumulate_400_holders():
    assert _acc()["holders_above_400"] == 85   # 50+20+10+5


def test_accumulate_600_holders():
    assert _acc()["holders_above_600"] == 35   # 20+10+5


def test_accumulate_800_holders():
    assert _acc()["holders_above_800"] == 15   # 10+5


def test_accumulate_1000_holders():
    assert _acc()["holders_above_1000"] == 5


def test_accumulate_400_pct():
    assert abs(_acc()["pct_above_400"] - 24.5) < 0.01   # 6.25+3.50+2.25+12.50


def test_accumulate_total():
    acc = _acc()
    assert acc["total_holders"] == 100_085
    assert acc["total_shares"] == 148_000_000


def test_empty_rows_returns_zeros():
    from whaleflow.fetchers.tdcc_scraper import _accumulate
    result = _accumulate([{"level": "合計", "persons": 0, "shares": 0, "pct": 0.0}])
    assert result["holders_above_400"] == 0
    assert result["pct_above_400"] == 0.0


def test_missing_level_treated_as_zero():
    rows = [{"level": "1-999", "persons": 1000, "shares": 500, "pct": 1.0},
            {"level": "合計", "persons": 1000, "shares": 500, "pct": 100.0}]
    acc = _accumulate(rows)
    assert acc["holders_above_400"] == 0
    assert acc["pct_above_400"] == 0.0
