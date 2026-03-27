"""個股趨勢頁：顯示單支股票的 TDCC 集保分佈歷史趨勢。"""

import streamlit as st

from whaleflow.dashboard.charts import (
    build_pct_trend_chart,
    build_price_volume_chart,
    build_shares_trend_chart,
)
from whaleflow.dashboard.data_loader import (
    get_all_stocks,
    get_stock_info,
    get_tdcc_history,
    get_price_history,
)

st.set_page_config(page_title="WhaleFlow · 個股趨勢", layout="wide", page_icon="📈")

# ── Sidebar ────────────────────────────────────────────────────────────────────

st.sidebar.header("🔍 選股")

all_stocks = get_all_stocks()
if not all_stocks:
    st.error("DB 中無股票資料，請先執行 `fetch stocks`。")
    st.stop()

options = [f"{sid} {name}" for sid, name in all_stocks]

# 優先從 session_state 取（掃描結果頁跳轉），其次讀 query param（書籤/直連）
default_stock = st.session_state.pop("selected_stock", None) or st.query_params.get("stock", "")
default_idx = 0
if default_stock:
    for i, (sid, _) in enumerate(all_stocks):
        if sid == default_stock:
            default_idx = i
            break

selected = st.sidebar.selectbox(
    "股票代號 / 名稱",
    options=options,
    index=default_idx,
)
stock_id = selected.split()[0]

limit_weeks = st.sidebar.slider("歷史週數", min_value=4, max_value=52, value=16, step=4)

st.sidebar.divider()

show_pct    = st.sidebar.toggle("持股比例趨勢", value=True)
show_shares = st.sidebar.toggle("持股張數趨勢", value=True)
show_price  = st.sidebar.toggle("收盤價 / 成交量", value=True)
show_raw    = st.sidebar.toggle("原始數據表格", value=False)

# ── 股票基本資訊 ───────────────────────────────────────────────────────────────

info_map = get_stock_info((stock_id,))
info     = info_map.get(stock_id, {})
stock_name = info.get("name", stock_id)

st.title(f"📈 {stock_id}  {stock_name}")

c1, c2, c3 = st.columns(3)
c1.metric("市場", info.get("market", "—"))
c2.metric("產業", info.get("industry", "—"))
c3.metric("代號", stock_id)

st.divider()

# ── 資料載入 ───────────────────────────────────────────────────────────────────

tdcc_df  = get_tdcc_history(stock_id, limit_weeks)
price_df = get_price_history(stock_id, limit_weeks)

if tdcc_df.empty:
    st.warning(f"找不到 {stock_id} 的 TDCC 集保資料。請先用 `fetch tdcc` 補充歷史資料。")
    st.stop()

# ── 圖表 ──────────────────────────────────────────────────────────────────────

if show_pct:
    st.subheader("大戶持股比例趨勢")
    st.plotly_chart(
        build_pct_trend_chart(tdcc_df, stock_name),
        use_container_width=True,
    )

if show_shares:
    st.subheader("大戶持股張數趨勢（千張）")
    st.plotly_chart(
        build_shares_trend_chart(tdcc_df, stock_name),
        use_container_width=True,
    )

if show_price:
    if price_df.empty:
        st.info("DB 中無此股票的週收盤價資料（請先執行 `scan weekly` 補充）。")
    else:
        st.subheader("收盤價與成交量")
        st.plotly_chart(
            build_price_volume_chart(price_df, stock_name),
            use_container_width=True,
        )

# ── 原始數據 ──────────────────────────────────────────────────────────────────

if show_raw:
    st.subheader("TDCC 原始數據")
    display = tdcc_df.copy()
    display["week_date"] = display["week_date"].dt.strftime("%Y-%m-%d")
    for col in ["shares_400", "shares_800", "shares_1000"]:
        display[col] = display[col].round(1)
    for col in ["pct_400", "pct_800", "pct_1000"]:
        display[col] = display[col].round(2)
    st.dataframe(display, use_container_width=True, hide_index=True)
