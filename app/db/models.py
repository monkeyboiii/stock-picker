from sqlalchemy import (
    Integer,
    String,
    Time,
    Date,
    Float,
    Numeric,
    BigInteger,
    ForeignKey,
    PrimaryKeyConstraint,
)
from sqlalchemy.orm import Mapped, DeclarativeBase
from sqlalchemy.orm import mapped_column
from sqlalchemy.inspection import inspect

from app.db.engine import engine_from_env


class MetadataBase(DeclarativeBase):
    pass


class Market(MetadataBase):
    '''

    Stores gloabl market information
    '''
    __tablename__ = 'market'

    id:             Mapped[int] = mapped_column(Integer, primary_key=True)
    name:           Mapped[str] = mapped_column(String)
    name_short:     Mapped[str] = mapped_column(String, nullable=True)
    country:        Mapped[str] = mapped_column(String, nullable=True)
    
    open:           Mapped[Time] = mapped_column(Time, nullable=True)
    break_start:    Mapped[Time] = mapped_column(Time, nullable=True)
    break_end:      Mapped[Time] = mapped_column(Time, nullable=True)
    close:          Mapped[Time] = mapped_column(Time, nullable=True)

    currency:       Mapped[str]


class Stock(MetadataBase):
    __tablename__ = 'stock'

    code:           Mapped[str] = mapped_column(String(10), primary_key=True)
    name:           Mapped[str] = mapped_column(String(50))

    #
    market_id:      Mapped[int] = mapped_column(ForeignKey('market.id'))


# TODO add stock section tables


class StockDaily(MetadataBase):
    __tablename__ = "stock_daily"
    __table_args__ = (
        PrimaryKeyConstraint('code', 'trade_day'),
    )

    # basic
    code:                       Mapped[str]         = mapped_column(ForeignKey('stock.code'))
    trade_day:                  Mapped[Date]        = mapped_column(Date)
    
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


class StockDailyFiltered(MetadataBase):
    '''
    Filtered stocks for each day. Insert back to db for showing and backtest.
    '''
    
    __tablename__ = "stock_daily_filtered"
    __table_args__ = (
        PrimaryKeyConstraint('code', 'trade_day'),
    )

    code:               Mapped[str]         = mapped_column(ForeignKey('stock.code'))
    trade_day:          Mapped[Date]        = mapped_column(Date)
    
    # convenient
    name:               Mapped[str]         = mapped_column(String)
    volume:             Mapped[BigInteger]  = mapped_column(BigInteger)
    close:              Mapped[Numeric]     = mapped_column(Numeric(10, 3))

    # derived
    increase_ratio:     Mapped[Float]       = mapped_column(Float)
    previous_close:     Mapped[Numeric]     = mapped_column(Numeric(10, 3))
    previous_volume:    Mapped[BigInteger]  = mapped_column(BigInteger)
    ma_5_volume:        Mapped[BigInteger]  = mapped_column(BigInteger)

    # backtest
    # TODO add backtest columns

    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in self.__table__.columns}

if __name__ == "__main__":
    # MetadataBase.metadata.create_all(engine_from_env())
    MetadataBase.metadata.tables['stock_daily_filtered'].create(engine_from_env())
