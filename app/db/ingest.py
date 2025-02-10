from time import sleep
from datetime import date, timedelta
from typing import Optional, Dict

from pandas import isna
from loguru import logger
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.ak.data import *
from app.constant.exchange import *
from app.constant.schedule import is_stock_market_open, previous_trade_day
from app.db.engine import engine_from_env
from app.db.models import Market, Stock, StockDaily


SLEEP_TIME_SECS = 0.1


def load_individual_stock_daily_hist(
    engine: Engine, 
    start_day_map: Dict[str, date] = {},
    end_date: Optional[date] = None,
) -> None:
    with Session(engine) as session:
        if end_date is None:
            end_date = date.today()
        
        for code, start_day in start_day_map.items():
            assert start_day <= end_date

            logger.debug(f"Getting daily data of {code} for {(end_date - start_day + timedelta(days=1)).days} days")

            df = pull_stock_daily_hist(
                symbol=code,
                start_date=start_day,
                end_date=end_date,
                adjust='qfq'
            )

            if len(df) == 0:
                logger.warning(f"No daily data for {code} from {start_day} to {end_date}")
                continue

            stock_objs = [
                StockDaily(
                    code=code,
                    trade_day=row['trade_day'],
                    open=row['open'],
                    high=row['high'],
                    low=row['low'],
                    close=row['close'],
                    volume=row['volume'],
                    turnover=row['turnover'],
                )
                for _, row in df.iterrows()
            ]
            
            session.add_all(stock_objs)
            session.commit()

            logger.info(f"Total of {len(stock_objs)} daily data for {code} committed")
            # TODO async
            
            sleep(SLEEP_TIME_SECS)


def load_all_stock_daily_hist(
    engine:     Engine,
    market_name: str,
    start_date: Optional[date] = None,
    end_date:   Optional[date] = None,
) -> None:
    if market_name not in market_map.keys():
        raise ValueError(f"exchange {market_name} not supported")


    with Session(engine) as session:

        market = session.execute(
            select(Market).where(Market.name_short == market_name)
        ).scalar_one_or_none()

        if market is not None:
            stocks = session.execute(
                select(Stock).where(Stock.market_id == market.id)
            )

            for stock_row in stocks:
                stock = stock_row[0]
                if end_date is None:
                    end_date = date.today()
                if start_date is None:
                    start_date = date(end_date.year - 2, end_date.month, end_date.day)

                logger.info(f"Getting daily data for {stock.name} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

                df = pull_stock_daily_hist(
                    symbol=stock.code,
                    start_date=start_date,
                    end_date=end_date,
                    adjust='qfq'
                )

                stock_objs = [
                    StockDaily(
                        code=stock.code,
                        trade_day=row['trade_day'],
                        open=row['open'],
                        high=row['high'],
                        low=row['low'],
                        close=row['close'],
                        volume=row['volume'],
                        turnover=row['turnover'],
                    )
                    for _, row in df.iterrows()
                ]

                session.add_all(stock_objs)
                session.commit()

                logger.info(f"Total of {len(stock_objs)} daily data for {stock.name} committed")
                # TODO async
                sleep(SLEEP_TIME_SECS)

        else:
            logger.error(f"Market {market_name} not in database")


def load_all_stocks(engine: Engine, market_name: str) -> None:
    if market_name not in market_map.keys():
        raise ValueError(f"exchange {market_name} not supported")


    with Session(engine) as session:
        market = session.execute(
            select(Market).where(Market.name_short == market_name)
        ).scalar_one_or_none()

        if market is not None:
            logger.info(f"Getting stocks info for {market_name}")

            df = pull_stock(market_name)
            for code, name in zip(df['代码'], df['名称']):
                stock = Stock(
                    code=code,
                    name=name,
                    market_id=market.id
                )
                session.add(stock)

            session.commit()
            logger.info(f"Total of {len(df)} stocks committed into {market_name}")

        else:
            logger.warning(f"Market {market_name} not in database")


def refresh_stock_daily(engine: Engine, today: Optional[date] = None) -> None:
    if today is None:
        today = previous_trade_day(date.today(), inclusive=True)

    if not is_stock_market_open(today):
        logger.error(f"Stock market is not open on {today.isoformat()}")
        return


    with Session(engine) as session:
        df = pull_stock_daily(today)

        try:
            if engine.dialect.name == 'postgresql':
                # upsert statement
                data = df.to_dict(orient='records')
                stmt = pg_insert(StockDaily).values(data)
                update_dict = {
                    'open': stmt.excluded.open,
                    'high': stmt.excluded.high,
                    'low': stmt.excluded.low,
                    'close': stmt.excluded.close,
                    'volume': stmt.excluded.volume,
                    'turnover': stmt.excluded.turnover,
                    'capital': stmt.excluded.capital,
                    'circulation_capital': stmt.excluded.circulation_capital,
                    'quantity_relative_ratio': stmt.excluded.quantity_relative_ratio,
                    'turnover_rate': stmt.excluded.turnover_rate,
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=['code', 'trade_day'],
                    set_=update_dict
                )
                session.execute(stmt)

            else:
                for _, row in df.iterrows():
                    one_stock = {key: (None if isna(value) else value) for key, value in row.to_dict().items()}
                    stock = StockDaily(
                        code=one_stock['code'],
                        trade_day=today,
                        open=one_stock['open'],
                        high=one_stock['high'],
                        low=one_stock['low'],
                        close=one_stock['close'],
                        volume=one_stock['volume'],
                        turnover=one_stock['turnover'],
                        capital=one_stock['capital'],
                        circulation_capital=one_stock['circulation_capital'],
                        quantity_relative_ratio=one_stock['quantity_relative_ratio'],
                        turnover_rate=one_stock['turnover_rate'],
                    )
                    session.merge(stock)


            session.commit()
            logger.info(f"Total of {len(df)} daily data committed for {today.isoformat()}")

        except Exception as e:
            session.rollback()
            logger.error(f"Error in committing daily data for {today.isoformat()}: {e}")


if __name__ == '__main__':
    supported_markets = [SEX_SHANGHAI, SEX_SHENZHEN, SEX_BEIJING]
    unsupported_markets = [SEX_HONGKONG]
    engine = engine_from_env()

    # for market_name in unsupported_markets:
    # for market_name in supported_markets:
    #     load_all_stocks(engine, market_name)

    # load 
    # daily data for the past 2 years
    # or
    # today's data
    # for market_name in unsupported_markets:
    # for market_name in supported_markets:
    #     load_all_stock_daily_hist(engine, market_name, start_date=date(2025, 1, 28))

    # load daily
    refresh_stock_daily(engine)
