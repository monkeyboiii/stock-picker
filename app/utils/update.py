from typing import Optional
from datetime import date

from loguru import logger
from sqlalchemy import select, update, func, true, and_
from sqlalchemy import Select
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
    s = Stock.__table__.alias('s')
    sd = StockDaily.__table__.alias('sd')

    last_250_inner_subq = (
        select(StockDaily.close)
            .select_from(StockDaily)
            .where(and_(
                StockDaily.code == s.c.code,
                StockDaily.trade_day <= trade_day,
            ))
            .order_by(StockDaily.trade_day.desc())
            .limit(250)
            .correlate(s)
    ).alias('innermost')

    # lateral subquery rhs
    ma250_subq = (
        select(func.avg(last_250_inner_subq.c.close))
            .scalar_subquery()
    ).label("ma250")

    row_count_subq = (
        select(func.count(last_250_inner_subq.c.close))
            .scalar_subquery()
    ).label("row_count")
    
    # lateral query
    lateral_query = (
        select(
            ma250_subq,
            row_count_subq,
        )
        .select_from(StockDaily)
        .where(and_(
            StockDaily.code == s.c.code,
            StockDaily.trade_day == trade_day,
        ))
        .correlate(s)
    )

    latest_lateral_subq = lateral(lateral_query).alias('latest')

    stmt = (
        select(
            s.c.code,
            s.c.name,
            latest_lateral_subq.c.ma250.label('ma_250')
        )
        .select_from(s)
        .join(sd, s.c.code == sd.c.code)
        .join(latest_lateral_subq, true())
        .where(and_(
            sd.c.trade_day == trade_day,
            latest_lateral_subq.c.row_count == 250
        ))
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
        results = session.execute(stmt).mappings().all()

        if dryrun:
            logger.info(f"Calculated a total of {len(results)} ma_250")
            return

        ma_250_dict = {row['code']: row['ma_250'] for row in results}

        session.execute(update(StockDaily), 
            [
                {'code': code, 'trade_day': trade_day, 'ma_250': value}
                for code, value in ma_250_dict.items()
            ]
        )

        session.commit()

        logger.success(f"Updated a total of {len(results)} ma_250 for {trade_day} in db")


def calculate_ma250_materialized_view(engine: Engine, trade_day: Optional[date] = None, dryrun: Optional[bool] = False) -> None:
    pass


# def calculate_ma250(engine: Engine, trade_day: Optional[date] = None, dryrun: Optional[bool] = False) -> None:
    # pass


if __name__ == '__main__':
    from app.constant.schedule import previous_trade_day

    trade_day = previous_trade_day(date(2025, 2, 21))
    calculate_ma250(
        engine_from_env(), 
        trade_day, 
        dryrun=False,
    )