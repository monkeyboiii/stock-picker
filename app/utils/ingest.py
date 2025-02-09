from datetime import date
from typing import Optional

from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine

from app.ak.data import *
from app.constant.exchange import *
from app.constant.schedule import *
from app.db.engine import engine_from_env
from app.db.models import Stock, StockDaily
from app.db.ingest import load_individual_stock_daily_hist


def auto_fill_hist(engine: Engine, up_to_date: Optional[date] = None) -> None:
    if up_to_date is None:
        up_to_date = date.today()

    up_to_date = previous_trade_day(up_to_date, inclusive=True)
    logger.info(f"Auto fill history data up to {up_to_date.isoformat()}")

    with Session(engine) as session:
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

        start_day_map = {}
        
        for row in db_latest_dates:
            code, db_latest_trade_day = row

            if db_latest_trade_day is None:
                continue
            
            supposed_next_trade_day = next_trade_day(db_latest_trade_day, inclusive=False)
            if supposed_next_trade_day > up_to_date:
                continue

            start_day_map[code] = supposed_next_trade_day
        
        load_individual_stock_daily_hist(engine, start_day_map, up_to_date)



if __name__ == '__main__':
    auto_fill_hist(engine_from_env())
