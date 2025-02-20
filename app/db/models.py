from typing import List

from sqlalchemy import (
    Integer,
    String,
    Time,
    Date,
    Float,
    Numeric,
    DateTime,
    BigInteger,
    ForeignKey,
    UniqueConstraint,
    PrimaryKeyConstraint,
)
from sqlalchemy import func
from sqlalchemy.orm import Mapped, DeclarativeBase
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy.types import Enum as SQLAlchemyEnum
from sqlalchemy.inspection import inspect

from app.constant.collection import CollectionType


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
    stocks:                     Mapped[List["Stock"]]   = relationship(
                                "Stock", secondary='relation_collection_stock', back_populates="collections")


class Stock(MetadataBase):
    __tablename__ = 'stock'
    __table_args__ = UniqueConstraint("name"),

    code:                       Mapped[str]         = mapped_column(String(10), primary_key=True)
    name:                       Mapped[str]         = mapped_column(String(50))

    # relations
    market_id:                  Mapped[int]         = mapped_column(ForeignKey('market.id'))

    collections:                Mapped[List["Collection"]] = relationship(
                                "Collection", secondary='relation_collection_stock', back_populates="stocks")


class RelationCollectionStock(MetadataBase):
    __tablename__ = 'relation_collection_stock'
    __table_args__ = PrimaryKeyConstraint('collection_code', 'stock_code'),

    collection_code:            Mapped[int]         = mapped_column(ForeignKey('collection.code'))
    stock_code:                 Mapped[str]         = mapped_column(ForeignKey('stock.code'))


class CollectionDaily(MetadataBase):
    __tablename__ = 'collection_daily'
    __table_args__ = PrimaryKeyConstraint('code', 'trade_day'),

    code:                       Mapped[int]         = mapped_column(ForeignKey('collection.code'))
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
    code:                       Mapped[str]         = mapped_column(ForeignKey('stock.code'))
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

    code:                       Mapped[str]         = mapped_column(ForeignKey('stock.code'))
    trade_day:                  Mapped[Date]        = mapped_column(Date)
    filter_id:                  Mapped[int]         = mapped_column(Integer, default=0)
    last_updated:               Mapped[DateTime]    = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # convenient
    name:                       Mapped[str]         = mapped_column(String)
    collection_name:            Mapped[BigInteger]  = mapped_column(String, nullable=True)
    collection_performance:     Mapped[Float]       = mapped_column(Float, nullable=True)
    close:                      Mapped[Numeric]     = mapped_column(Numeric(10, 3))
    previous_close:             Mapped[Numeric]     = mapped_column(Numeric(10, 3))
    
    # derived
    gain:                       Mapped[Float]       = mapped_column(Float)
    
    # fill later
    next_open:                  Mapped[Numeric]     = mapped_column(Numeric(10, 3), nullable=True)
    next_close:                 Mapped[Numeric]     = mapped_column(Numeric(10, 3), nullable=True)

    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in self.__table__.columns}


if __name__ == "__main__":
    from app.db.engine import *

    # MetadataBase.metadata.create_all(engine_from_env(echo=True))
    MetadataBase.metadata.tables['feed_daily'].create(engine_from_env(echo=True))

    # MetadataBase.metadata.create_all(engine_mock(echo=True))