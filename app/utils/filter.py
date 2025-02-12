from typing import Optional
from datetime import date, datetime

from sqlalchemy import select, func, and_, true
from sqlalchemy import Select
from sqlalchemy.orm import Session
from sqlalchemy.sql import lateral
from sqlalchemy.engine import Engine
from loguru import logger
from pandas import DataFrame

from app.ak.data import *
from app.constant.exchange import *
from app.constant.schedule import previous_trade_day
from app.db.engine import engine_from_env
from app.db.models import Stock, StockDaily, StockDailyFiltered


def build_stmt_postgresql(trade_day: date) -> Select:
    '''
    Based on T1~8 conditions.
    '''

    static_filtering_cte = (
        select(Stock, StockDaily)
            .select_from(Stock).join(StockDaily, Stock.code == StockDaily.code)
            .where(and_(
                StockDaily.trade_day == trade_day,

                # T2
                StockDaily.quantity_relative_ratio >= 1,

                # T3
                StockDaily.turnover_rate > 5.0, # scaled

                # T4
                StockDaily.circulation_capital.between(2_0000_0000, 200_0000_0000),

                # T6
                Stock.name.not_ilike('%ST%'),
                Stock.name.not_like('%*%'),

                # T7
                StockDaily.low > StockDaily.ma_250,

                # T8
                StockDaily.close > StockDaily.open,
            ))
            .cte("static_filtering")
    )

    prev_subq = lateral(
        select(StockDaily.close.label("close"))
        .where(
            StockDaily.code == static_filtering_cte.c.code,
            StockDaily.trade_day < static_filtering_cte.c.trade_day
        )
        .order_by(StockDaily.trade_day.desc())
        .limit(1)
    )

    innermost = (
        select(StockDaily.volume)
            .where(and_(
                StockDaily.code == static_filtering_cte.c.code,
                StockDaily.trade_day <= static_filtering_cte.c.trade_day
            ))
            .order_by(StockDaily.trade_day.desc())
            .correlate(static_filtering_cte)
            .limit(5)
    ).subquery()

    vol_ma5_expr = select(func.avg(innermost.c.volume)).scalar_subquery()

    vol_prev_day_expr = (
        select(StockDaily.volume)
            .where(
                StockDaily.code == static_filtering_cte.c.code,
                StockDaily.trade_day < static_filtering_cte.c.trade_day
            )
            .order_by(StockDaily.trade_day.desc())
            .correlate(static_filtering_cte)
            .limit(1)
    ).scalar_subquery()

    vol_prev_subq = lateral(
        select(
            vol_ma5_expr.label("vol_ma5"),
            vol_prev_day_expr.label("vol_prev_day")
        )
        .where(
            StockDaily.code == static_filtering_cte.c.code,
            StockDaily.trade_day == trade_day
        )
    )

    stmt = (
        select(
            static_filtering_cte.c.code,
            static_filtering_cte.c.name,
            static_filtering_cte.c.trade_day,
            static_filtering_cte.c.close,
            prev_subq.c.close.label("previous_close"),
            func.round(100.0 * (static_filtering_cte.c.close / prev_subq.c.close - 1), 3).label("increase_ratio"),
            static_filtering_cte.c.volume,
            vol_prev_subq.c.vol_prev_day.label("previous_volume"),
            func.round(vol_prev_subq.c.vol_ma5).label("ma_5_volume"),
        )
        .select_from(static_filtering_cte)
        .join(prev_subq, true())
        .join(vol_prev_subq, true())
        .where(
            # T1
            func.round(100.0 * (static_filtering_cte.c.close / prev_subq.c.close - 1), 3).between(3, 5),
            
            # T5
            static_filtering_cte.c.volume > vol_prev_subq.c.vol_ma5,
            vol_prev_subq.c.vol_prev_day  < vol_prev_subq.c.vol_ma5,
        )

        # Tx
        .order_by(static_filtering_cte.c.code.desc())
    )

    return stmt


def filter_desired(engine: Engine, trade_day: Optional[date] = None, dryrun: Optional[bool] = False) -> DataFrame:
    output = []
    output_columns = StockDailyFiltered.__table__.columns.keys()

    if trade_day is None:
        trade_day = previous_trade_day(date.today(), inclusive=True)

    if engine.dialect.name == 'postgresql':
        stmt = build_stmt_postgresql(trade_day)
        logger.debug(stmt.compile(engine, compile_kwargs={"literal_binds": True}))
    else:
        raise Exception('Not implemented!')

    matched_count = 0
    with Session(engine) as session:
        start = datetime.now()
        results = session.execute(stmt)
        
        for result in results:
            sdf = StockDailyFiltered(**result._mapping)
            output.append(sdf.to_dict())
            if not dryrun:
                session.add(sdf)
            matched_count += 1
        
        if not dryrun:
            session.commit()
        logger.info(f"Found {matched_count} matching records.")
    
        elapsed_ms = round((datetime.now() - start).total_seconds() * 1000)
        logger.info(f"Filtering desired stocks completed in {elapsed_ms} ms")

    return DataFrame(output, columns=output_columns) if results else DataFrame()



if __name__ == '__main__':
    trade_day = date(2025, 2, 10)
    df = filter_desired(engine_from_env(), trade_day, dryrun=True)
    df.to_csv(f'reports/report-{trade_day.isoformat()}.csv')
    # df.to_excel(f'report/report-{trade_day.isoformat()}.xlsx')
    print(df)