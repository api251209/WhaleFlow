"""掃描結果頁：顯示最新集保趨勢選股結果。"""

import streamlit as st

from whaleflow.dashboard.config_reader import load_scan_config
from whaleflow.dashboard.data_loader import (
    build_scan_dataframe,
    get_available_weeks,
    run_scan,
)

st.set_page_config(page_title="WhaleFlow · 掃描結果", layout="wide", page_icon="📊")


# ── Sidebar ────────────────────────────────────────────────────────────────────

def _sidebar(cfg):
    st.sidebar.header("⚙️ 掃描設定")

    available = get_available_weeks()
    if not available:
        st.error("DB 中無 TDCC 資料，請先執行：\n```\npoetry run python -m whaleflow fetch all\n```")
        st.stop()

    target_week = st.sidebar.selectbox(
        "目標週",
        options=available,
        format_func=lambda d: str(d),
    )

    st.sidebar.divider()

    min_score = st.sidebar.slider(
        "最低總分門檻",
        min_value=0.0, max_value=7.0, step=0.5,
        value=float(cfg.min_total_score),
    )

    conditions = st.sidebar.multiselect(
        "啟用條件",
        options=["A", "B", "C"],
        default=[k for k, v in cfg.enabled.items() if v],
    )

    top_n = st.sidebar.number_input("顯示前 N 名", min_value=5, max_value=200, value=30)

    st.sidebar.divider()

    price_filter = st.sidebar.toggle("週漲幅過濾", value=cfg.price_filter_enabled)
    max_gain_pct = st.sidebar.number_input(
        "最高週漲幅（%）",
        min_value=1, max_value=100, value=int(cfg.max_weekly_gain * 100),
        disabled=not price_filter,
    )

    liq_filter = st.sidebar.toggle("流動性過濾", value=cfg.liquidity_filter_enabled)
    min_vol = st.sidebar.number_input(
        "最低日均量（張）",
        min_value=100, max_value=50000, step=100,
        value=cfg.min_avg_daily_volume,
        disabled=not liq_filter,
    )

    return target_week, min_score, conditions, int(top_n), price_filter, max_gain_pct / 100, liq_filter, min_vol


# ── 主體 ───────────────────────────────────────────────────────────────────────

cfg = load_scan_config()
target_week, min_score, conditions, top_n, price_filter, max_gain, liq_filter, min_vol = _sidebar(cfg)

# 套用 sidebar 覆蓋
from copy import deepcopy
scan_cfg = deepcopy(cfg)
scan_cfg.min_total_score = min_score
scan_cfg.enabled = {"A": "A" in conditions, "B": "B" in conditions, "C": "C" in conditions}
scan_cfg.price_filter_enabled = price_filter
scan_cfg.max_weekly_gain = max_gain
scan_cfg.liquidity_filter_enabled = liq_filter
scan_cfg.min_avg_daily_volume = min_vol

st.title("📊 集保趨勢選股掃描結果")

with st.spinner("掃描中..."):
    signals = run_scan(target_week, scan_cfg)

# ── 頂部 Metrics ───────────────────────────────────────────────────────────────

col1, col2, col3, col4 = st.columns(4)
col1.metric("候選股數", len(signals))
col2.metric("掃描週", str(target_week))
col3.metric("最高分", f"{signals[0].score:.1f}" if signals else "—")
col4.metric("最低分門檻", f"{min_score:.1f}")

st.divider()

# ── 結果表格 ───────────────────────────────────────────────────────────────────

if not signals:
    st.info("本週無符合條件的個股。請嘗試調低篩選門檻，或確認 DB 有足夠週數的 TDCC 資料。")
    st.stop()

df = build_scan_dataframe(signals[:top_n], target_week)

# 格式化顯示欄位
display_df = df.copy()
display_df["週漲跌%"] = display_df["週漲跌%"].apply(
    lambda x: f"{x:+.1f}%" if x is not None else "N/A"
)
display_df["法人淨買(張)"] = display_df["法人淨買(張)"].apply(
    lambda x: f"{x:+,}" if x is not None else "N/A"
)
display_df["成交量(張)"] = display_df["成交量(張)"].apply(
    lambda x: f"{x:,}" if x is not None else "N/A"
)

event = st.dataframe(
    display_df.drop(columns=["說明"]),
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    column_config={
        "排名":         st.column_config.NumberColumn(width="small"),
        "代號":         st.column_config.TextColumn(width="small"),
        "名稱":         st.column_config.TextColumn(width="medium"),
        "產業":         st.column_config.TextColumn(width="medium"),
        "分數":         st.column_config.NumberColumn(format="%.1f", width="small"),
        "條件":         st.column_config.TextColumn(width="small"),
        "週漲跌%":      st.column_config.TextColumn(width="small"),
        "法人淨買(張)":  st.column_config.TextColumn(width="medium"),
        "成交量(張)":   st.column_config.TextColumn(width="medium"),
    },
)

st.caption(f"共 {len(signals)} 檔符合，顯示前 {min(top_n, len(signals))} 名 · 點擊列可查看個股趨勢")

# ── 點擊列 → 跳轉個股趨勢頁 ──────────────────────────────────────────────────

selected_rows = event.selection.rows if event else []
if selected_rows:
    idx = selected_rows[0]
    row = df.iloc[idx]
    stock_id = row["代號"]
    stock_name = row["名稱"]

    st.info(f"已選取：**{stock_id} {stock_name}**")
    if st.button("📈 查看個股趨勢圖", type="primary"):
        st.session_state["selected_stock"] = stock_id
        st.switch_page("pages/2_stock_trend.py")

# ── 說明折疊 ───────────────────────────────────────────────────────────────────

with st.expander("📋 條件說明", expanded=False):
    score_a = scan_cfg.scores.get("A", 3.0)
    score_b = scan_cfg.scores.get("B", 2.0)
    score_c = scan_cfg.scores.get("C", 2.5)
    a_cfg   = scan_cfg.scores.get("_a_cfg", {})
    b_cfg   = scan_cfg.scores.get("_b_cfg", {})
    c_cfg   = scan_cfg.scores.get("_c_cfg", {})
    st.markdown(f"""
| 條件 | 分數 | 說明 |
|------|------|------|
| A | {score_a:.1f} | 400張以上持股比例，T 週 > T-{a_cfg.get('lookback_weeks',4)-1} 週（淨增加），人數增加需 > {a_cfg.get('min_holder_delta',3)} 人，比例增加需 > {a_cfg.get('min_pct_delta',0.2)}% |
| B | {score_b:.1f} | 800張以上持股張數，連續 {b_cfg.get('consecutive_weeks',2)} 週增加，每週人數變動需 > {b_cfg.get('min_holder_delta',3)} 人，每週增加需 > {b_cfg.get('min_share_delta',200)} 張 |
| C | {score_c:.1f} | 1000張以上人數與張數，本週同時增加，人數增加需 > {c_cfg.get('min_holder_delta',3)} 人，張數增加需 > {c_cfg.get('min_share_delta',500)} 張 |

**欄位說明**
- **週漲跌%**：T-1週 → T週收盤價變化
- **法人淨買**：本週三大法人合計淨買超（正=買超 / 負=賣超）
- **成交量**：T 週收盤日成交量（張）
    """)

# ── 說明詳細 ──────────────────────────────────────────────────────────────────

with st.expander("📄 各股詳細說明", expanded=False):
    for _, row in df.iterrows():
        st.markdown(f"**{row['代號']} {row['名稱']}**（{row['條件']}，{row['分數']}分）  \n{row['說明']}")
