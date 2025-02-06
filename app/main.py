from sqlalchemy import select
from sqlalchemy.orm import Session
from loguru import logger
import pandas as pd

from db.engine import engine_from_env
from db.models import StockDaily


def main():
    engine = engine_from_env()
    trade_day = '2025-02-06'
    
    stock = StockDaily(
        code='688720',
        trade_day=trade_day,
        open=None,
        high=None,
        low=None,
        close=None,
        volume=None,
        capital=2343214,
        circulation_capital=None,
        quantity_relative_ratio=12.421,
    )
    with Session(engine) as session:
        result = session.execute(
            select(StockDaily).where(StockDaily.code == '688720').where(StockDaily.trade_day == trade_day)
        ).scalar_one_or_none()
        result = session.merge(stock)
        logger.info(f"Merged result: {result.to_dict() if result else 'None'}")
        session.commit()


if __name__ == "__main__":
    main()
