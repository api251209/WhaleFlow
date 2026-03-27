"""Unit tests for ETF detection and stock parsing logic."""

from whaleflow.services.stock_service import is_etf


def test_etf_by_id_pattern():
    assert is_etf("0050", "元大台灣50", "") is True
    assert is_etf("00878", "國泰永續高股息", "") is True
    assert is_etf("006208", "富邦台50", "") is True


def test_etf_by_industry():
    assert is_etf("1234", "某某基金", "ETF") is True
    assert is_etf("1234", "某某憑證", "受益憑證") is True
    assert is_etf("1234", "某某槓桿", "槓桿及反向") is True


def test_etf_by_name_keyword():
    assert is_etf("1234", "台灣ETF基金", "") is True
    assert is_etf("1234", "債券型基金", "") is True


def test_regular_stock_not_etf():
    assert is_etf("2330", "台積電", "半導體") is False
    assert is_etf("2317", "鴻海", "電子零組件") is False
    assert is_etf("2412", "中華電信", "通信網路") is False
    assert is_etf("3008", "大立光", "光電") is False
