from typing import Optional
from datetime import date, datetime

from sqlalchemy import select, func, and_, true
from sqlalchemy import Table, Select, Double
from sqlalchemy.orm import Session
from sqlalchemy.sql import lateral
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from loguru import logger
from pandas import DataFrame

from app.constant.exchange import *
from app.constant.schedule import previous_trade_day
from app.db.engine import engine_from_env
from app.db.materialized_view import MV_STOCK_DAILY, check_mv_exists
from app.db.models import *
from app.filter.misc import StockFilter, get_filter_id
from app.profile.tracer import trace_elapsed


def build_stmt_postgresql_lateral(trade_day: date) -> Select:
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
            func.round(100.0 * (static_filtering_cte.c.close / prev_subq.c.close - 1), 3).label("gain"),
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


def build_stmt_postgresql_mv(mv_stock_daily: Table, trade_day: date) -> Select:

    prev = mv_stock_daily.alias("prev")
    sd = StockDaily.__table__.alias("sd")
    s = Stock.__table__.alias("s")
    rcs = RelationCollectionStock.__table__.alias("rcs")
    c = Collection.__table__.alias("c")
    cd = CollectionDaily.__table__.alias("cd")

    stmt  = (
        select(
            sd.c.trade_day,
            sd.c.code,
            s.c.name,
            c.c.name.label("collection_name"),
            cd.c.change_rate.label("collection_performance"),
            # func.round(prev.c.ma250 + (sd.c.close - prev.c.prev_250_close) / 250, 3).label("ma_250"),
            prev.c.close.label("previous_close"),
            sd.c.close.label("close"),
            (100 * (sd.c.close / prev.c.close - 1)).label("gain"),
            # func.round(prev.c.ma5_volume + (sd.c.volume - prev.c.prev_5_volume) / 5).label("ma5_volume"),
            prev.c.volume.label("previous_volume"),
            sd.c.volume.label("volume"),
            (100 * (sd.c.volume.cast(Double) / prev.c.volume.cast(Double) - 1)).label("volume_gain"),
        )
        .join(sd, prev.c.code == sd.c.code)
        .join(s, sd.c.code == s.c.code)
        .join(rcs, s.c.code == rcs.c.stock_code)
        .join(c, c.c.code == rcs.c.collection_code)
        .join(cd, (c.c.code == cd.c.code) & (sd.c.trade_day == cd.c.trade_day))
        .where(
            # T0
            sd.c.trade_day == trade_day,

            # T1
            (100.0 * (sd.c.close / prev.c.close - 1)).between(3, 5),

            # T2
            sd.c.quantity_relative_ratio >= 1,

            # T3
            sd.c.turnover_rate > 5.0,

            # T4
            sd.c.circulation_capital.between(2_0000_0000, 200_0000_0000),

            # T5
            prev.c.volume < prev.c.ma5_volume + (sd.c.volume - prev.c.prev_5_volume) / 5,
            sd.c.volume   > prev.c.ma5_volume + (sd.c.volume - prev.c.prev_5_volume) / 5,

            # T6
            ~s.c.name.like("%ST%"),
            ~s.c.name.like("%*%"),

            # T7
            sd.c.low > prev.c.ma250 + (sd.c.close - prev.c.prev_250_close) / 250,

            # T8
            sd.c.close > sd.c.open,
        )
        # TX
        .order_by(func.round(100.0 * (sd.c.close / prev.c.close - 1), 3).desc())  
    )

    return stmt



def build_stmt_postgresql(engine: Engine, trade_day: date) -> Select:
    if check_mv_exists(engine, MV_STOCK_DAILY):
        mv_stock_daily = Table(MV_STOCK_DAILY, MetadataBase.metadata, autoload_with=engine)
        logger.debug("Filter using materialized view")
        return build_stmt_postgresql_mv(mv_stock_daily, trade_day)
    else:
        logger.debug("Filter using lateral join")
        return build_stmt_postgresql_lateral(trade_day)


@trace_elapsed(unit='s')
def filter_desired(engine: Engine, trade_day: Optional[date] = None, dryrun: Optional[bool] = False) -> DataFrame:
    output = []
    output_columns = FeedDaily.__table__.columns.keys()

    if trade_day is None:
        trade_day = previous_trade_day(date.today(), inclusive=True)

    if engine.dialect.name == 'postgresql':
        filter_stmt = build_stmt_postgresql(engine, trade_day)
        logger.debug(filter_stmt.compile(engine, compile_kwargs={"literal_binds": True}))
    else:
        raise Exception('Not implemented!')

    with Session(engine) as session:
        results = session.execute(filter_stmt)
        
        for result in results:
            fd = FeedDaily(
                filter_id=get_filter_id(StockFilter.TAIL_SCRAPER),
                **result._mapping
            )
            output.append(fd)

        if not results:
            return DataFrame()
        
        df = DataFrame([fd.to_dict() for fd in output], columns=output_columns)
        df['last_updated'] = datetime.now()
        
        if not dryrun:
            if engine.dialect.name == 'postgresql':
                data = df.to_dict(orient='records')
                insert_stmt = pg_insert(FeedDaily).values(data)
                update_dict = {
                    'name': insert_stmt.excluded.name,
                    'collection_name': insert_stmt.excluded.collection_name,
                    'collection_performance': insert_stmt.excluded.collection_performance,
                    'previous_close': insert_stmt.excluded.previous_close,
                    'close': insert_stmt.excluded.close,
                    'gain': insert_stmt.excluded.gain,
                    'previous_volume': insert_stmt.excluded.previous_volume,
                    'volume': insert_stmt.excluded.volume,
                    'volume_gain': insert_stmt.excluded.volume_gain,
                }
                insert_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=['code', 'trade_day', 'filter_id'],
                    set_=update_dict
                )
                session.execute(insert_stmt)
            else:
                for fd in output:
                    session.merge(fd)
            session.commit()
        logger.info(f"Found {len(output)} matching records.")
    
    return df



if __name__ == '__main__':
    trade_day = date(2025, 2, 20)
    df = filter_desired(engine_from_env(), trade_day, dryrun=False)
    df.to_csv(f'reports/report-{trade_day.isoformat()}.csv')
    # df.to_excel(f'report/report-{trade_day.isoformat()}.xlsx')
    print(df)