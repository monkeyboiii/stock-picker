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

    code:       Mapped[str] = mapped_column(String(10), primary_key=True)
    name:       Mapped[str] = mapped_column(String(50))

    #
    market_id:  Mapped[int] = mapped_column(ForeignKey('market.id'))


class StockDaily(MetadataBase):
    __tablename__ = "stock_daily"
    __table_args__ = (
        PrimaryKeyConstraint('code', 'trade_day'),
    )

    # basic
    code:       Mapped[str] = mapped_column(ForeignKey('stock.code'))
    trade_day:  Mapped[Date] = mapped_column(Date)
    
    # price
    open:   Mapped[Numeric] = mapped_column(Numeric(10, 3))
    high:   Mapped[Numeric] = mapped_column(Numeric(10, 3))
    low:    Mapped[Numeric] = mapped_column(Numeric(10, 3))
    close:  Mapped[Numeric] = mapped_column(Numeric(10, 3))
    
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
    ma_250: Mapped[Float] = mapped_column(Float, nullable=True)


if __name__ == "__main__":
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL
    from dotenv import load_dotenv
    import os


    load_dotenv()

    url = URL.create(
        drivername= os.getenv("DB_DRIVER")          or 'postgresql',
        username=   os.getenv("POSTGRES_USERNAME")  or 'postgres',
        password=   os.getenv("POSTGRES_PASSWORD")  or 'postgres',
        host=       os.getenv("POSTGRES_HOST")      or 'localhost',
        port=   int(os.getenv("POSTGRES_PORT")      or '5432'),
        database=   os.getenv("POSTGRES_DATABASE")
    )

    engine = create_engine(url)
    
    MetadataBase.metadata.create_all(engine)
