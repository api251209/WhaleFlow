from datetime import date, datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Stock(Base):
    __tablename__ = "stocks"

    stock_id: Mapped[str] = mapped_column(String(10), primary_key=True)
    stock_name: Mapped[str] = mapped_column(String(50), nullable=False)
    market: Mapped[str] = mapped_column(String(10), nullable=False)  # TWSE / TPEx
    industry: Mapped[str | None] = mapped_column(String(30))
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_etf: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    updated_at: Mapped[datetime] = mapped_column(default=func.now(), onupdate=func.now())


class TdccDistribution(Base):
    """集保分佈 -- 每週每檔股票一筆，400/600/800/1000 張以上各累計。"""

    __tablename__ = "tdcc_distribution"

    stock_id: Mapped[str] = mapped_column(String(10), primary_key=True)
    week_date: Mapped[date] = mapped_column(Date, primary_key=True)

    # 400 張以上
    holders_above_400: Mapped[int | None] = mapped_column(Integer)
    shares_above_400: Mapped[int | None] = mapped_column(BigInteger)
    pct_above_400: Mapped[float | None] = mapped_column(Float)

    # 600 張以上
    holders_above_600: Mapped[int | None] = mapped_column(Integer)
    shares_above_600: Mapped[int | None] = mapped_column(BigInteger)
    pct_above_600: Mapped[float | None] = mapped_column(Float)

    # 800 張以上
    holders_above_800: Mapped[int | None] = mapped_column(Integer)
    shares_above_800: Mapped[int | None] = mapped_column(BigInteger)
    pct_above_800: Mapped[float | None] = mapped_column(Float)

    # 1000 張以上
    holders_above_1000: Mapped[int | None] = mapped_column(Integer)
    shares_above_1000: Mapped[int | None] = mapped_column(BigInteger)
    pct_above_1000: Mapped[float | None] = mapped_column(Float)

    total_holders: Mapped[int | None] = mapped_column(Integer)
    total_shares: Mapped[int | None] = mapped_column(BigInteger)

    created_at: Mapped[datetime] = mapped_column(default=func.now())

    __table_args__ = (Index("idx_tdcc_week_date", "week_date"),)


class WeeklyPrice(Base):
    """每週收盤價（週五）。掃描時自動更新，用於計算週漲跌幅。"""

    __tablename__ = "weekly_price"

    stock_id: Mapped[str] = mapped_column(String(10), primary_key=True)
    trade_date: Mapped[date] = mapped_column(Date, primary_key=True)
    close_price: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[int | None] = mapped_column(BigInteger)  # 成交量（張）
    created_at: Mapped[datetime] = mapped_column(default=func.now())

    __table_args__ = (Index("idx_weekly_price_date", "trade_date"),)


class InstitutionalTrading(Base):
    """三大法人每日買賣超。"""

    __tablename__ = "institutional_trading"

    stock_id: Mapped[str] = mapped_column(String(10), primary_key=True)
    trade_date: Mapped[date] = mapped_column(Date, primary_key=True)

    foreign_buy: Mapped[int] = mapped_column(BigInteger, default=0)
    foreign_sell: Mapped[int] = mapped_column(BigInteger, default=0)
    foreign_net: Mapped[int] = mapped_column(BigInteger, default=0)

    investment_trust_buy: Mapped[int] = mapped_column(BigInteger, default=0)
    investment_trust_sell: Mapped[int] = mapped_column(BigInteger, default=0)
    investment_trust_net: Mapped[int] = mapped_column(BigInteger, default=0)

    dealer_buy: Mapped[int] = mapped_column(BigInteger, default=0)
    dealer_sell: Mapped[int] = mapped_column(BigInteger, default=0)
    dealer_net: Mapped[int] = mapped_column(BigInteger, default=0)

    total_net: Mapped[int] = mapped_column(BigInteger, default=0)

    created_at: Mapped[datetime] = mapped_column(default=func.now())

    __table_args__ = (Index("idx_institutional_date", "trade_date"),)


class MarginTrading(Base):
    """融資融券每日資料。"""

    __tablename__ = "margin_trading"

    stock_id: Mapped[str] = mapped_column(String(10), primary_key=True)
    trade_date: Mapped[date] = mapped_column(Date, primary_key=True)

    margin_buy: Mapped[int] = mapped_column(BigInteger, default=0)
    margin_sell: Mapped[int] = mapped_column(BigInteger, default=0)
    margin_balance: Mapped[int] = mapped_column(BigInteger, default=0)
    margin_balance_change: Mapped[int] = mapped_column(BigInteger, default=0)

    short_buy: Mapped[int] = mapped_column(BigInteger, default=0)
    short_sell: Mapped[int] = mapped_column(BigInteger, default=0)
    short_balance: Mapped[int] = mapped_column(BigInteger, default=0)
    short_balance_change: Mapped[int] = mapped_column(BigInteger, default=0)

    created_at: Mapped[datetime] = mapped_column(default=func.now())

    __table_args__ = (Index("idx_margin_date", "trade_date"),)


class MonthlyRevenue(Base):
    """月營收。"""

    __tablename__ = "monthly_revenue"

    stock_id: Mapped[str] = mapped_column(String(10), primary_key=True)
    revenue_year: Mapped[int] = mapped_column(Integer, primary_key=True)
    revenue_month: Mapped[int] = mapped_column(Integer, primary_key=True)

    revenue: Mapped[int] = mapped_column(BigInteger, nullable=False)
    revenue_yoy: Mapped[float | None] = mapped_column(Float)
    revenue_mom: Mapped[float | None] = mapped_column(Float)

    created_at: Mapped[datetime] = mapped_column(default=func.now())


class EtfHolding(Base):
    """ETF 成分股對照表。"""

    __tablename__ = "etf_holdings"

    etf_id: Mapped[str] = mapped_column(String(10), primary_key=True)
    stock_id: Mapped[str] = mapped_column(String(10), primary_key=True)
    weight: Mapped[float | None] = mapped_column(Float)
    updated_date: Mapped[date] = mapped_column(Date, nullable=False)

    __table_args__ = (Index("idx_etf_stock", "stock_id"),)


class ScanResult(Base):
    """策略掃描結果快照。"""

    __tablename__ = "scan_results"

    stock_id: Mapped[str] = mapped_column(String(10), primary_key=True)
    scan_date: Mapped[date] = mapped_column(Date, primary_key=True)

    score: Mapped[float] = mapped_column(Float, nullable=False)

    pct_trend: Mapped[float | None] = mapped_column(Float)
    holder_trend: Mapped[float | None] = mapped_column(Float)
    shares_trend: Mapped[float | None] = mapped_column(Float)
    institutional: Mapped[float | None] = mapped_column(Float)
    margin_stable: Mapped[float | None] = mapped_column(Float)
    price_stable: Mapped[float | None] = mapped_column(Float)

    config_json: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(default=func.now())

    __table_args__ = (Index("idx_scan_date_score", "scan_date", "score"),)


class FetchLog(Base):
    """資料抓取日誌。"""

    __tablename__ = "fetch_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset: Mapped[str] = mapped_column(String(50), nullable=False)
    fetch_date: Mapped[date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(10), nullable=False)  # success/failed/partial
    records_count: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text)
    duration_sec: Mapped[float | None] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(default=func.now())

    __table_args__ = (Index("idx_fetch_log_dataset_date", "dataset", "fetch_date"),)


class StrategyConfig(Base):
    """使用者策略設定（單列）。"""

    __tablename__ = "strategy_config"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, default=1, server_default="1"
    )
    consecutive_weeks: Mapped[int] = mapped_column(Integer, default=3)
    threshold_level: Mapped[int] = mapped_column(Integer, default=400)

    weight_pct_trend: Mapped[float] = mapped_column(Float, default=0.30)
    weight_holder_trend: Mapped[float] = mapped_column(Float, default=0.15)
    weight_shares_trend: Mapped[float] = mapped_column(Float, default=0.15)
    weight_institutional: Mapped[float] = mapped_column(Float, default=0.20)
    weight_margin_stable: Mapped[float] = mapped_column(Float, default=0.10)
    weight_price_stable: Mapped[float] = mapped_column(Float, default=0.10)

    margin_surge_threshold: Mapped[float] = mapped_column(Float, default=10.0)
    price_surge_threshold: Mapped[float] = mapped_column(Float, default=15.0)
    min_score: Mapped[float] = mapped_column(Float, default=60.0)

    updated_at: Mapped[datetime] = mapped_column(default=func.now(), onupdate=func.now())

    __table_args__ = (CheckConstraint("id = 1", name="single_row"),)
