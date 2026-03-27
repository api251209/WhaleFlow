"""WhaleFlow Dashboard 入口。

啟動方式：
    poetry run streamlit run whaleflow/dashboard/app.py
或：
    poetry run python -m whaleflow dashboard
"""

import streamlit as st

st.set_page_config(
    page_title="WhaleFlow",
    page_icon="🐋",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.sidebar.markdown("## 🐋 WhaleFlow")
st.sidebar.markdown("台灣集保大戶追蹤選股系統")
st.sidebar.divider()

st.title("🐋 WhaleFlow")
st.markdown("### 台灣集保大戶持股追蹤選股系統")
st.markdown("""
請從左側選單選擇功能：

- **📊 掃描結果** — 查看最新選股結果，可調整篩選條件
- **📈 個股趨勢** — 查看單支股票的大戶持股歷史趨勢圖

---

**資料更新方式（CLI）：**
```bash
# 每週例行更新
poetry run python -m whaleflow fetch all

# 執行掃描（順便更新股價）
poetry run python -m whaleflow scan weekly --save
```
""")
