from datetime import date, datetime
from typing import Optional

from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine

from app.constant.exchange import *
from app.constant.schedule import *
from app.constant.confirm import confirms_execution
from app.db.engine import engine_from_env
from app.db.models import Stock, StockDaily
from app.db.ingest import load_individual_stock_daily_hist, refresh_stock_daily


def auto_fill(engine: Engine, up_to_date: Optional[date] = None) -> None:
    '''
    Auto fill history data up to a specific date for all stocks.
    '''

    if up_to_date is None:
        up_to_date = date.today()

    up_to_date = previous_trade_day(up_to_date, inclusive=True)
    logger.info(f"Auto fill history data up to {up_to_date.isoformat()}")

    confirms_execution()

    with Session(engine) as session:
        start = datetime.now()

        latest_trade_day_subquery = (
            select(StockDaily.trade_day)
                .where(StockDaily.code == Stock.code)
                .order_by(StockDaily.trade_day.desc())
                .limit(1)
                .correlate(Stock)
                .scalar_subquery()
        )

        db_latest_dates = session.execute(
            select(
                Stock.code,
                StockDaily.trade_day.label('latest_trade_day')
            ).join(StockDaily, Stock.code == StockDaily.code)
            .where(StockDaily.trade_day == latest_trade_day_subquery)
        ).fetchall()

        # fill hist only when gap is >= 2 days
        start_day_map = {}
        # refresh stock daily if only 1 day missing
        start_day_map_single = {}
        
        for row in db_latest_dates:
            code, db_latest_trade_day = row

            if db_latest_trade_day is None:
                continue
            
            supposed_next_trade_day = next_trade_day(db_latest_trade_day, inclusive=False)
            if supposed_next_trade_day > up_to_date:
                continue

            if up_to_date - supposed_next_trade_day >= timedelta(days=2):
                start_day_map[code] = supposed_next_trade_day
            else:
                start_day_map_single[code] = supposed_next_trade_day
        
        load_individual_stock_daily_hist(engine, start_day_map, up_to_date)
        refresh_stock_daily(engine, up_to_date)

        elapsed_ms = round((datetime.now() - start).total_seconds() * 1000)
        logger.info(f"Auto fill history data completed in {elapsed_ms} ms")



if __name__ == '__main__':
    auto_fill(engine_from_env())
