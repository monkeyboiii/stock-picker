from typing import Optional
from datetime import datetime, date

from loguru import logger
from sqlalchemy import select, func, true, and_
from sqlalchemy import Select
from sqlalchemy.orm import load_only
from sqlalchemy.orm import Session
from sqlalchemy.sql import lateral
from sqlalchemy.engine import Engine

from app.constant.misc import *
from app.constant.exchange import *
from app.constant.schedule import is_stock_market_open
from app.db.engine import engine_from_env
from app.db.models import Stock, StockDaily
from app.profile.tracer import trace_elapsed


def build_stmt_postgresql(trade_day: date) -> Select:
    # inner most
    last_250_inner_subq = (
        select(StockDaily.close)
            .where(and_(
                StockDaily.code == Stock.code,
                StockDaily.trade_day <= trade_day,
            ))
            .order_by(StockDaily.trade_day.desc())
            .limit(250)
            .correlate(Stock)
    ).alias('innermost')

    # lateral subquery rhs
    ma250_subq = (
        select(func.avg(last_250_inner_subq.c.close))
            .scalar_subquery()
    ).label("ma250")

    row_count = (
        select(func.count(last_250_inner_subq.c.close))
            .scalar_subquery()
    ).label("row_count")
    
    # lateral query
    lateral_query = (
        select(ma250_subq, row_count)
            .where(and_(StockDaily.code == Stock.code, StockDaily.trade_day == trade_day))
            .correlate(Stock)
    )

    latest_lateral_subq = lateral(lateral_query).alias('latest')

    stmt = (
        select(
            Stock.code,
            Stock.name,
            func.round(latest_lateral_subq.c.ma250, 3).label('ma250')
        )
        .join(StockDaily, Stock.code == StockDaily.code)
        .select_from(Stock).join(latest_lateral_subq, true())
        .where(and_(
            StockDaily.trade_day == trade_day,
            latest_lateral_subq.c.row_count == 250
        ))
        .order_by(Stock.code)
    )

    return stmt


@trace_elapsed(unit='s')
def calculate_ma250(engine: Engine, trade_day: Optional[date] = None, dryrun: Optional[bool] = False) -> None:
    if trade_day is None:
        trade_day = date.today()

    assert is_stock_market_open(trade_day)

    # build query
    if engine.dialect.name == 'postgresql':
        stmt = build_stmt_postgresql(trade_day) 
    else:
        raise Exception("Not implemented!")
    logger.debug(stmt.compile(engine, compile_kwargs={"literal_binds": True}))

    # execute
    with Session(engine) as session:
        results = session.execute(stmt)
        count = 0
        for row in results:
            stock_daily_obj = session.query(
                StockDaily
            ).options(
                load_only(StockDaily.code)
            ).filter(
                StockDaily.code == row.code
            ).filter(
                StockDaily.trade_day == trade_day
            ).first()

            if stock_daily_obj:
                count += 1
                if not dryrun:
                    stock_daily_obj.ma_250 = row.ma250
                logger.debug(f'Stock ({row.code}, {row.name}) has ma_250 of {row.ma250} on {trade_day.isoformat()}')

        if not dryrun:
            session.commit()
            
            logger.info(f"Updated a total of {count} ma_250 for {trade_day} in db")
        else:
            logger.warning(f"Showed a total of {count} ma_250")


def calculate_ma250_materialized_view(engine: Engine, trade_day: Optional[date] = None, dryrun: Optional[bool] = False) -> None:
    pass


# def calculate_ma250(engine: Engine, trade_day: Optional[date] = None, dryrun: Optional[bool] = False) -> None:
    # pass


if __name__ == '__main__':
    from app.constant.schedule import previous_trade_day

    trade_day = previous_trade_day(date(2025, 2, 20))
    calculate_ma250(
        engine_from_env(), 
        trade_day, 
        dryrun=False,
    )