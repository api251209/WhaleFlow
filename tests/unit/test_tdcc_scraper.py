"""Unit tests for TDCC scraper parsing logic (pure functions, no HTTP)."""

from whaleflow.fetchers.tdcc_scraper import _accumulate, _parse_table

# Minimal HTML that mimics the TDCC result page (2 tables)
_TABLE_HTML_VALID = """
<table><tr><td>form</td></tr></table>
<table>
  <tr><th>序</th><th>持股/單位數分級</th><th>人數</th><th>股數/單位數</th><th>占集保庫存數比例 (%)</th></tr>
  <tr><td>12</td><td>400,001-600,000</td><td>570</td><td>279,178,158</td><td>1.07</td></tr>
  <tr><td>13</td><td>600,001-800,000</td><td>352</td><td>244,932,366</td><td>0.94</td></tr>
  <tr><td>14</td><td>800,001-1,000,000</td><td>214</td><td>192,235,384</td><td>0.74</td></tr>
  <tr><td>15</td><td>1,000,001以上</td><td>1,513</td><td>22,213,562,001</td><td>85.65</td></tr>
  <tr><td>16</td><td>合　計</td><td>2,464,986</td><td>25,932,524,521</td><td>100.00</td></tr>
</table>
"""

_TABLE_HTML_NO_DATA = """
<table><tr><td>form</td></tr></table>
<table>
  <tr><th>序</th><th>持股/單位數分級</th><th>人數</th><th>股數/單位數</th><th>占集保庫存數比例 (%)</th></tr>
  <tr><td>查無此資料</td></tr>
</table>
"""


def test_parse_table_valid():
    rows = _parse_table(_TABLE_HTML_VALID)
    assert rows is not None
    levels = {r["level"] for r in rows}
    assert "400,001-600,000" in levels
    assert "1,000,001以上" in levels
    assert "合計" in levels


def test_parse_table_no_data():
    assert _parse_table(_TABLE_HTML_NO_DATA) is None


def test_parse_table_no_second_table():
    assert _parse_table("<table><tr><td>only one</td></tr></table>") is None


def test_accumulate_400():
    rows = _parse_table(_TABLE_HTML_VALID)
    acc = _accumulate(rows)
    # 400+ = 570 + 352 + 214 + 1513 = 2649
    assert acc["holders_above_400"] == 2649


def test_accumulate_600():
    rows = _parse_table(_TABLE_HTML_VALID)
    acc = _accumulate(rows)
    # 600+ = 352 + 214 + 1513 = 2079
    assert acc["holders_above_600"] == 2079


def test_accumulate_1000():
    rows = _parse_table(_TABLE_HTML_VALID)
    acc = _accumulate(rows)
    assert acc["holders_above_1000"] == 1513


def test_accumulate_pct_400():
    rows = _parse_table(_TABLE_HTML_VALID)
    acc = _accumulate(rows)
    # 1.07 + 0.94 + 0.74 + 85.65 = 88.40
    assert abs(acc["pct_above_400"] - 88.40) < 0.01


def test_accumulate_total():
    rows = _parse_table(_TABLE_HTML_VALID)
    acc = _accumulate(rows)
    assert acc["total_holders"] == 2_464_986
    assert acc["total_shares"] == 25_932_524_521
