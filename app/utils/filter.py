from typing import Optional

from sqlalchemy import select, func, and_, not_
from sqlalchemy import Select
from sqlalchemy.orm import load_only
from sqlalchemy.orm import Session
from sqlalchemy.sql import lateral
from sqlalchemy.engine import Engine
from loguru import logger
from pandas import DataFrame

from app.ak.data import *
from app.constant.exchange import *
from app.constant.schedule import is_stock_market_open
from app.db.engine import engine_from_env
from app.db.models import Stock, StockDaily



def filter_out_desired(engine: Engine, trade_day: Optional[date] = None) -> DataFrame:
    if trade_day is None:
        trade_day = date.today()

    assert is_stock_market_open(trade_day)

    increase = func.lag(StockDaily.close).over(order_by=StockDaily.trade_day.desc())

    stmt = (
        select(StockDaily, Stock.name, increase.label("increase"))
            .join(Stock, Stock.code == StockDaily.code)
            .where(and_(
                StockDaily.trade_day == trade_day,

                # T2
                StockDaily.quantity_relative_ratio >= 1,

                # T3
                StockDaily.turnover_rate > 5.0, # scaled

                # T4
                StockDaily.circulation_capital.between(2_0000_0000, 200_0000_0000),

                # T6
                Stock.name.not_ilike('%ST$'),
                Stock.name.not_like('%*$'),

                # T7
                StockDaily.close > StockDaily.ma_250,

                # T8
                StockDaily.close > StockDaily.open,
            ))
    )

    logger.debug(stmt.compile(engine, compile_kwargs={"literal_binds": True}))

    return DataFrame()



if __name__ == '__main__':
    df = filter_out_desired(engine_from_env(), date(2025, 2, 7))
    print(df)