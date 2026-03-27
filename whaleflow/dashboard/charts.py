"""Plotly 圖表工廠函數。"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def build_pct_trend_chart(df: pd.DataFrame, stock_name: str) -> go.Figure:
    """
    大戶持股比例趨勢折線圖（雙 Y 軸）。

    左軸：pct_above_400 / 800 / 1000（%）
    右軸：holders_above_400 / 1000（人數）
    """
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # 持股比例（左軸）
    for col, label, color in [
        ("pct_400",  "400張以上%", "#2196F3"),
        ("pct_800",  "800張以上%", "#FF9800"),
        ("pct_1000", "1000張以上%", "#F44336"),
    ]:
        if col in df.columns:
            fig.add_trace(
                go.Scatter(x=df["week_date"], y=df[col], name=label,
                           line=dict(color=color, width=2), mode="lines+markers",
                           marker=dict(size=5)),
                secondary_y=False,
            )

    # 持股人數（右軸）
    for col, label, color, dash in [
        ("holders_400",  "400張以上人數", "#2196F3", "dot"),
        ("holders_1000", "1000張以上人數", "#F44336", "dot"),
    ]:
        if col in df.columns:
            fig.add_trace(
                go.Scatter(x=df["week_date"], y=df[col], name=label,
                           line=dict(color=color, width=1.5, dash=dash), mode="lines",
                           opacity=0.7),
                secondary_y=True,
            )

    fig.update_layout(
        title=f"{stock_name} 大戶持股比例趨勢",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400,
        margin=dict(l=0, r=0, t=50, b=0),
    )
    fig.update_yaxes(title_text="持股比例（%）", secondary_y=False, ticksuffix="%")
    fig.update_yaxes(title_text="人數（人）", secondary_y=True)
    return fig


def build_shares_trend_chart(df: pd.DataFrame, stock_name: str) -> go.Figure:
    """大戶持股張數趨勢折線圖（單位：千張）。"""
    fig = go.Figure()

    for col, label, color in [
        ("shares_400",  "400張以上（千張）", "#2196F3"),
        ("shares_800",  "800張以上（千張）", "#FF9800"),
        ("shares_1000", "1000張以上（千張）", "#F44336"),
    ]:
        if col in df.columns:
            fig.add_trace(go.Scatter(
                x=df["week_date"], y=df[col], name=label,
                line=dict(color=color, width=2), mode="lines+markers",
                marker=dict(size=5),
            ))

    fig.update_layout(
        title=f"{stock_name} 大戶持股張數趨勢",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=380,
        margin=dict(l=0, r=0, t=50, b=0),
        yaxis_title="千張",
    )
    return fig


def build_price_volume_chart(
    price_df: pd.DataFrame,
    stock_name: str,
) -> go.Figure:
    """收盤價（折線）+ 成交量（長條）組合圖。"""
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        row_heights=[0.7, 0.3],
        vertical_spacing=0.05,
    )

    if not price_df.empty and "close_price" in price_df.columns:
        fig.add_trace(
            go.Scatter(x=price_df["trade_date"], y=price_df["close_price"],
                       name="收盤價", line=dict(color="#4CAF50", width=2),
                       mode="lines+markers", marker=dict(size=4)),
            row=1, col=1,
        )

    if not price_df.empty and "volume" in price_df.columns:
        fig.add_trace(
            go.Bar(x=price_df["trade_date"], y=price_df["volume"],
                   name="成交量(張)", marker_color="#90CAF9"),
            row=2, col=1,
        )

    fig.update_layout(
        title=f"{stock_name} 收盤價與成交量",
        hovermode="x unified",
        showlegend=True,
        height=380,
        margin=dict(l=0, r=0, t=50, b=0),
    )
    fig.update_yaxes(title_text="價格（元）", row=1, col=1)
    fig.update_yaxes(title_text="張", row=2, col=1)
    return fig
