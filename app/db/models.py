from __future__ import annotations

from datetime import datetime

from pandas import DataFrame
from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Text,
    Time,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import Enum as SQLAlchemyEnum

from app.constant.collection import CollectionType
from app.constant.trading import DEFAULT_COMMISSION_RATE, DEFAULT_INITIAL_CAPITAL, DEFAULT_SLIPPAGE_RATE
from app.display.utils import ten_thousand_format


class MetadataBase(DeclarativeBase):
    pass


class Market(MetadataBase):
    '''
    Stores gloabl market information
    '''

    __tablename__ = 'market'

    id:                         Mapped[int]         = mapped_column(Integer, primary_key=True)
    name:                       Mapped[str]         = mapped_column(String)
    name_short:                 Mapped[str]         = mapped_column(String, nullable=True)
    country:                    Mapped[str]         = mapped_column(String, nullable=True)

    open:                       Mapped[Time]        = mapped_column(Time, nullable=True)
    break_start:                Mapped[Time]        = mapped_column(Time, nullable=True)
    break_end:                  Mapped[Time]        = mapped_column(Time, nullable=True)
    close:                      Mapped[Time]        = mapped_column(Time, nullable=True)

    currency:                   Mapped[str]         = mapped_column(String, nullable=True)


class Collection(MetadataBase):
    '''
    A collection of stocks, such as concept board, industry board, index, analyst, etc.

    - type:                     {'a', 'b', 'c', 'i', 'x'}
    '''

    __tablename__ = 'collection'

    code:                       Mapped[str]         = mapped_column(String(30), primary_key=True)
    name:                       Mapped[str]         = mapped_column(String)
    type:                       Mapped[CollectionType]  = mapped_column(SQLAlchemyEnum(CollectionType))

    #
    stocks:                     Mapped[list[Stock]]     = relationship(
                                "Stock", secondary='relation_collection_stock', back_populates="collections")


class Stock(MetadataBase):
    __tablename__ = 'stock'
    __table_args__ = UniqueConstraint("name"),

    code:                       Mapped[str]         = mapped_column(String(10), primary_key=True)
    name:                       Mapped[str]         = mapped_column(String(50))

    # relations
    market_id:                  Mapped[int]         = mapped_column(ForeignKey('market.id', ondelete='CASCADE'))

    collections:                Mapped[list[Collection]] = relationship(
                                "Collection", secondary='relation_collection_stock', back_populates="stocks")


class RelationCollectionStock(MetadataBase):
    __tablename__ = 'relation_collection_stock'
    __table_args__ = PrimaryKeyConstraint('collection_code', 'stock_code'),

    collection_code:            Mapped[int]         = mapped_column(ForeignKey('collection.code', ondelete='CASCADE'))
    stock_code:                 Mapped[str]         = mapped_column(ForeignKey('stock.code', ondelete='CASCADE'))


class CollectionDaily(MetadataBase):
    __tablename__ = 'collection_daily'
    __table_args__ = PrimaryKeyConstraint('code', 'trade_day'),

    code:                       Mapped[int]         = mapped_column(ForeignKey('collection.code', ondelete='CASCADE'))
    trade_day:                  Mapped[Date]        = mapped_column(Date)
    last_updated:               Mapped[DateTime]    = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    price:                      Mapped[Numeric]     = mapped_column(Numeric(10, 3), nullable=True)
    change:                     Mapped[Numeric]     = mapped_column(Numeric(10, 3), nullable=True)
    change_rate:                Mapped[Float]       = mapped_column(Float, nullable=True)
    capital:                    Mapped[BigInteger]  = mapped_column(BigInteger, nullable=True)
    turnover_rate:              Mapped[Float]       = mapped_column(Float, nullable=True)
    gainer_count:               Mapped[int]         = mapped_column(Integer, nullable=True)
    loser_count:                Mapped[int]         = mapped_column(Integer, nullable=True)
    top_gainer:                 Mapped[str]         = mapped_column(String(50), nullable=True)
    top_gain:                   Mapped[Float]       = mapped_column(Float, nullable=True)


class StockDaily(MetadataBase):
    __tablename__ = "stock_daily"
    __table_args__ = PrimaryKeyConstraint('code', 'trade_day'),

    # basic
    code:                       Mapped[str]         = mapped_column(ForeignKey('stock.code', ondelete='CASCADE'))
    trade_day:                  Mapped[Date]        = mapped_column(Date)
    last_updated:               Mapped[DateTime]    = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    # price
    open:                       Mapped[Numeric]     = mapped_column(Numeric(10, 3), nullable=True)
    high:                       Mapped[Numeric]     = mapped_column(Numeric(10, 3), nullable=True)
    low:                        Mapped[Numeric]     = mapped_column(Numeric(10, 3), nullable=True)
    close:                      Mapped[Numeric]     = mapped_column(Numeric(10, 3), nullable=True)

    # trade
    # 交易量
    volume:                     Mapped[BigInteger]  = mapped_column(BigInteger, nullable=True)
    # 交易额
    turnover:                   Mapped[BigInteger]  = mapped_column(BigInteger, nullable=True)
    # 总市值
    capital:                    Mapped[BigInteger]  = mapped_column(BigInteger, nullable=True)
    # 流通市值
    circulation_capital:        Mapped[BigInteger]  = mapped_column(BigInteger, nullable=True)
    # 量比
    quantity_relative_ratio:    Mapped[Float]       = mapped_column(Float, nullable=True)
    # 换手率
    turnover_rate:              Mapped[Float]       = mapped_column(Float, nullable=True)

    # derived
    # moving average
    ma_250:                     Mapped[Float]       = mapped_column(Float, nullable=True)

    def to_dict(self, level=1):
        if level == 0:
            return {c.key: getattr(self, c.key) for c in self.__table__.columns}
        elif level == 1:
            return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}
        else:
            return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}


class FeedDaily(MetadataBase):
    '''
    Filtered stocks for each day. Insert back to db for showing and backtest.
    '''

    __tablename__ = "feed_daily"
    __table_args__ = PrimaryKeyConstraint('code', 'trade_day', 'filter_id'),

    code:                       Mapped[str]         = mapped_column(ForeignKey('stock.code', ondelete='CASCADE'))
    trade_day:                  Mapped[Date]        = mapped_column(Date)
    filter_id:                  Mapped[int]         = mapped_column(Integer, default=0)
    last_updated:               Mapped[DateTime]    = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    # convenient
    name:                       Mapped[str]         = mapped_column(String)
    collection_name:            Mapped[String]      = mapped_column(String, nullable=True)
    collection_performance:     Mapped[Float]       = mapped_column(Float, nullable=True)
    previous_close:             Mapped[Numeric]     = mapped_column(Numeric(10, 3))
    close:                      Mapped[Numeric]     = mapped_column(Numeric(10, 3))
    previous_volume:            Mapped[BigInteger]  = mapped_column(BigInteger)
    volume:                     Mapped[BigInteger]  = mapped_column(BigInteger)

    # derived
    gain:                       Mapped[Float]       = mapped_column(Float)
    volume_gain:                Mapped[Float]       = mapped_column(Float)

    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in self.__table__.columns}

    @classmethod
    def to_dataframe(cls, fds: list[FeedDaily] = []) -> DataFrame:
        df = DataFrame(
            [fd.to_dict() for fd in fds],
            columns=FeedDaily.__table__.columns.keys(),
        )
        df["last_updated"] = datetime.now()
        return df

    @classmethod
    def feed_column_mapping(cls) -> dict:
        column_mapping = {
            'trade_day':                '交易日',
            'code':                     '股票代码',
            'name':                     '股票名称',
            'collection_name':          '板块名称',
            'collection_performance':   '板块表现',
            'previous_close':           '昨日收盘价',
            'close':                    '现价',
            'gain':                     '涨幅',
            'previous_volume':          '昨日成交量',
            'volume':                   '今日成交量',
            'volume_gain':              '量涨幅',
        }
        return column_mapping

    @classmethod
    def convert_to_feed(cls, df: DataFrame) -> DataFrame:
        column_mapping = FeedDaily.feed_column_mapping()
        transformations = {
            'collection_performance':   lambda x: format(x, '.2f') + '%',
            'gain':                     lambda x: format(x, '.2f') + '%',
            'previous_close':           lambda x: format(x, '.2f'),
            'close':                    lambda x: format(x, '.2f'),
            'previous_volume':          lambda x: ten_thousand_format(x),
            'volume':                   lambda x: ten_thousand_format(x),
            'volume_gain':              lambda x: format(x, '.2f') + '%',
        }

        for col, func_ in transformations.items():
            df[col] = df[col].apply(func_)

        return df.rename(columns=column_mapping)[list(column_mapping.values())]

    @classmethod
    def right_align_columns(cls) -> list[str]:
        columns = [
            'collection_performance',
            'previous_close',
            'close',
            'gain',
            'previous_volume',
            'volume',
            'volume_gain',
            # '板块表现',
            # '昨日收盘价',
            # '现价',
            # '涨幅',
            # '昨日成交量',
            # '今日成交量',
            # '量涨幅',
        ]
        # return [df.columns.get_loc(col) for col in columns] # type: ignore
        return columns

    @classmethod
    def colorize_columns(cls) -> list[str]:
        columns = [
            'collection_performance',
            'gain',
            'volume_gain',
            # '板块表现',
            # '涨幅',
            # '量涨幅',
        ]
        # return [df.columns.get_loc(col) for col in columns] # type: ignore
        return columns


class Strategy(MetadataBase):
    '''
    Stores strategy definitions for backtesting.

    A strategy defines entry/exit conditions, risk management rules,
    and position sizing logic using JSON/YAML format.
    '''

    __tablename__ = 'strategy'
    __table_args__ = UniqueConstraint("name", "version"),

    id:                         Mapped[str]         = mapped_column(UUID(as_uuid=False), primary_key=True, server_default=func.gen_random_uuid())
    name:                       Mapped[str]         = mapped_column(String(255))
    version:                    Mapped[str]         = mapped_column(String(50))
    author:                     Mapped[str]         = mapped_column(String(255), nullable=True)
    description:                Mapped[str]         = mapped_column(Text, nullable=True)

    # Strategy definition as JSON
    definition:                 Mapped[dict]        = mapped_column(JSON, nullable=False)

    # Metadata
    created_at:                 Mapped[DateTime]    = mapped_column(DateTime, server_default=func.now())
    updated_at:                 Mapped[DateTime]    = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
    is_active:                  Mapped[bool]        = mapped_column(Boolean, default=True)

    # Relationships
    backtest_runs:              Mapped[list["BacktestRun"]] = relationship("BacktestRun", back_populates="strategy", cascade="all, delete-orphan")


class BacktestRun(MetadataBase):
    '''
    Stores backtest run configuration and results.

    Each run represents a single execution of a strategy over a date range.
    Results (total_return, sharpe_ratio, etc.) are computed after completion.
    '''

    __tablename__ = 'backtest_run'
    __table_args__ = (
        CheckConstraint('initial_capital > 0', name='check_initial_capital_positive'),
        CheckConstraint('start_date <= end_date', name='check_date_range_valid'),
        CheckConstraint('commission_rate >= 0', name='check_commission_rate_non_negative'),
        CheckConstraint('slippage_rate >= 0', name='check_slippage_rate_non_negative'),
    )

    id:                         Mapped[str]         = mapped_column(UUID(as_uuid=False), primary_key=True, server_default=func.gen_random_uuid())
    strategy_id:                Mapped[str]         = mapped_column(UUID(as_uuid=False), ForeignKey('strategy.id', ondelete='CASCADE'))

    # Date range
    start_date:                 Mapped[Date]        = mapped_column(Date)
    end_date:                   Mapped[Date]        = mapped_column(Date)
    initial_capital:            Mapped[Numeric]     = mapped_column(Numeric(15, 2), default=DEFAULT_INITIAL_CAPITAL)

    # Configuration
    commission_rate:            Mapped[Numeric]     = mapped_column(Numeric(5, 4), default=DEFAULT_COMMISSION_RATE)
    slippage_rate:              Mapped[Numeric]     = mapped_column(Numeric(5, 4), default=DEFAULT_SLIPPAGE_RATE)

    # Results (computed after run)
    total_return:               Mapped[Numeric]     = mapped_column(Numeric(10, 4), nullable=True)
    annualized_return:          Mapped[Numeric]     = mapped_column(Numeric(10, 4), nullable=True)
    sharpe_ratio:               Mapped[Numeric]     = mapped_column(Numeric(10, 4), nullable=True)
    sortino_ratio:              Mapped[Numeric]     = mapped_column(Numeric(10, 4), nullable=True)
    calmar_ratio:               Mapped[Numeric]     = mapped_column(Numeric(10, 4), nullable=True)
    max_drawdown:               Mapped[Numeric]     = mapped_column(Numeric(10, 4), nullable=True)
    volatility:                 Mapped[Numeric]     = mapped_column(Numeric(10, 4), nullable=True)
    win_rate:                   Mapped[Numeric]     = mapped_column(Numeric(5, 4), nullable=True)
    profit_factor:              Mapped[Numeric]     = mapped_column(Numeric(10, 4), nullable=True)
    total_trades:               Mapped[int]         = mapped_column(Integer, nullable=True)

    # Status
    status:                     Mapped[str]         = mapped_column(String(20), default='pending')  # pending, running, completed, failed
    error_message:              Mapped[str]         = mapped_column(Text, nullable=True)

    # Timestamps
    started_at:                 Mapped[DateTime]    = mapped_column(DateTime, nullable=True)
    completed_at:               Mapped[DateTime]    = mapped_column(DateTime, nullable=True)
    created_at:                 Mapped[DateTime]    = mapped_column(DateTime, server_default=func.now())

    # Relationships
    strategy:                   Mapped["Strategy"]  = relationship("Strategy", back_populates="backtest_runs")
    trades:                     Mapped[list["Trade"]] = relationship("Trade", back_populates="backtest_run", cascade="all, delete-orphan")
    portfolio_snapshots:        Mapped[list["PortfolioSnapshot"]] = relationship("PortfolioSnapshot", back_populates="backtest_run", cascade="all, delete-orphan")


class Trade(MetadataBase):
    '''
    Stores individual trade records from backtests.

    Each trade represents a complete round trip: entry -> exit.
    Includes P&L calculation with costs (commission, slippage).
    '''

    __tablename__ = 'trade'
    __table_args__ = (
        CheckConstraint('shares > 0', name='check_shares_positive'),
        CheckConstraint('entry_price > 0', name='check_entry_price_positive'),
        CheckConstraint('exit_price IS NULL OR exit_price > 0', name='check_exit_price_positive'),
        CheckConstraint('exit_date IS NULL OR exit_date >= entry_date', name='check_exit_after_entry'),
    )

    id:                         Mapped[int]         = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    backtest_run_id:            Mapped[str]         = mapped_column(UUID(as_uuid=False), ForeignKey('backtest_run.id', ondelete='CASCADE'))
    stock_code:                 Mapped[str]         = mapped_column(String(10))
    stock_name:                 Mapped[str]         = mapped_column(String(50), nullable=True)

    # Entry
    entry_date:                 Mapped[Date]        = mapped_column(Date)
    entry_price:                Mapped[Numeric]     = mapped_column(Numeric(10, 3))
    entry_signal:               Mapped[str]         = mapped_column(Text, nullable=True)  # JSON of matched conditions

    # Exit
    exit_date:                  Mapped[Date]        = mapped_column(Date, nullable=True)
    exit_price:                 Mapped[Numeric]     = mapped_column(Numeric(10, 3), nullable=True)
    exit_reason:                Mapped[str]         = mapped_column(String(50), nullable=True)  # take_profit, stop_loss, time_limit, signal

    # Position
    shares:                     Mapped[int]         = mapped_column(Integer)
    position_value:             Mapped[Numeric]     = mapped_column(Numeric(15, 2), nullable=True)

    # P&L
    gross_pnl:                  Mapped[Numeric]     = mapped_column(Numeric(15, 2), nullable=True)
    commission:                 Mapped[Numeric]     = mapped_column(Numeric(15, 2), nullable=True)
    slippage:                   Mapped[Numeric]     = mapped_column(Numeric(15, 2), nullable=True)
    net_pnl:                    Mapped[Numeric]     = mapped_column(Numeric(15, 2), nullable=True)
    return_pct:                 Mapped[Numeric]     = mapped_column(Numeric(10, 4), nullable=True)

    # Metadata
    holding_days:               Mapped[int]         = mapped_column(Integer, nullable=True)
    collection_name:            Mapped[str]         = mapped_column(String(100), nullable=True)

    created_at:                 Mapped[DateTime]    = mapped_column(DateTime, server_default=func.now())

    # Relationships
    backtest_run:               Mapped["BacktestRun"] = relationship("BacktestRun", back_populates="trades")


class PortfolioSnapshot(MetadataBase):
    '''
    Stores daily portfolio state during backtests.

    Captures cash, holdings value, and performance metrics for each trading day.
    Used to generate equity curves and calculate drawdowns.
    '''

    __tablename__ = 'portfolio_snapshot'
    __table_args__ = (
        UniqueConstraint("backtest_run_id", "snapshot_date"),
        CheckConstraint('cash >= 0', name='check_cash_non_negative'),
        CheckConstraint('holdings_value >= 0', name='check_holdings_value_non_negative'),
        CheckConstraint('total_value >= 0', name='check_total_value_non_negative'),
    )

    id:                         Mapped[int]         = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    backtest_run_id:            Mapped[str]         = mapped_column(UUID(as_uuid=False), ForeignKey('backtest_run.id', ondelete='CASCADE'))
    snapshot_date:              Mapped[Date]        = mapped_column(Date)

    # Portfolio value
    cash:                       Mapped[Numeric]     = mapped_column(Numeric(15, 2))
    holdings_value:             Mapped[Numeric]     = mapped_column(Numeric(15, 2))
    total_value:                Mapped[Numeric]     = mapped_column(Numeric(15, 2))

    # Daily metrics
    daily_return:               Mapped[Numeric]     = mapped_column(Numeric(10, 4), nullable=True)
    cumulative_return:          Mapped[Numeric]     = mapped_column(Numeric(10, 4), nullable=True)
    drawdown:                   Mapped[Numeric]     = mapped_column(Numeric(10, 4), nullable=True)

    # Positions
    open_positions:             Mapped[int]         = mapped_column(Integer, nullable=True)
    total_positions_opened:     Mapped[int]         = mapped_column(Integer, nullable=True)
    total_positions_closed:     Mapped[int]         = mapped_column(Integer, nullable=True)

    created_at:                 Mapped[DateTime]    = mapped_column(DateTime, server_default=func.now())

    # Relationships
    backtest_run:               Mapped["BacktestRun"] = relationship("BacktestRun", back_populates="portfolio_snapshots")


class StrategyComparison(MetadataBase):
    '''
    Stores groups of backtest runs for side-by-side comparison.

    Allows users to compare multiple strategies or parameter variations.
    Includes comparison results and statistical significance tests.
    '''

    __tablename__ = 'strategy_comparison'

    id:                         Mapped[str]         = mapped_column(UUID(as_uuid=False), primary_key=True, server_default=func.gen_random_uuid())
    name:                       Mapped[str]         = mapped_column(String(255))
    created_at:                 Mapped[DateTime]    = mapped_column(DateTime, server_default=func.now())

    # Comparison metadata
    num_strategies:             Mapped[int]         = mapped_column(Integer)
    metrics_compared:           Mapped[list]        = mapped_column(JSON, nullable=True)  # JSON for SQLite compatibility

    # Results (stored as JSONB for flexibility)
    comparison_results:         Mapped[dict]        = mapped_column(JSON)
    statistical_tests:          Mapped[dict]        = mapped_column(JSON, nullable=True)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "id": str(self.id),
            "name": self.name,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "num_strategies": self.num_strategies,
            "metrics_compared": self.metrics_compared,
            "comparison_results": self.comparison_results,
            "statistical_tests": self.statistical_tests,
        }


class ComparisonBacktestRun(MetadataBase):
    '''
    Junction table linking strategy comparisons to backtest runs.

    Stores rank and additional metadata for each run in a comparison.
    '''

    __tablename__ = 'comparison_backtest_run'

    comparison_id:              Mapped[str]         = mapped_column(UUID(as_uuid=False), ForeignKey('strategy_comparison.id', ondelete='CASCADE'), primary_key=True)
    backtest_run_id:            Mapped[str]         = mapped_column(UUID(as_uuid=False), ForeignKey('backtest_run.id', ondelete='CASCADE'), primary_key=True)
    rank:                       Mapped[int]         = mapped_column(Integer, nullable=True)


if __name__ == "__main__":
    from app.constant.confirm import confirms_execution
    from app.db.engine import engine_from_env

    engine = engine_from_env(echo=True)

    # MetadataBase.metadata.create_all(engine)
    confirms_execution("Table operations", defaultYes=False)
    MetadataBase.metadata.tables['feed_daily'].drop(engine)
    MetadataBase.metadata.tables['feed_daily'].create(engine)

    # MetadataBase.metadata.create_all(engine_mock(echo=True))
