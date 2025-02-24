"""
Ingesting is for dynamic data like stock daily/collection daily
"""

from time import sleep
from datetime import date, timedelta
from typing import Optional, Dict

from pandas import isna
from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.constant.collection import CollectionType
from app.constant.exchange import MARKET_SUPPORTED
from app.constant.misc import TIME_SLEEP_SECS
from app.constant.schedule import is_stock_market_open, previous_trade_day
from app.data.ak import pull_collection_daily, pull_stock_daily, pull_stock_daily_hist
from app.db.engine import engine_from_env
from app.db.models import Collection, Market, Stock, StockDaily, CollectionDaily


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
            try:
                df = pull_stock_daily_hist(
                    symbol=code,
                    start_date=start_day,
                    end_date=end_date,
                    adjust='qfq'
                )
            except KeyError:
                logger.error(f'Got key error of stock code {code}, continuing...')
                session.commit()
                continue

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
            sleep(TIME_SLEEP_SECS)


def load_all_stock_daily_hist(
    engine:     Engine,
    market_name: str,
    start_date: Optional[date] = None,
    end_date:   Optional[date] = None,
) -> None:
    if market_name not in MARKET_SUPPORTED:
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

                logger.debug(f"Getting daily data for {stock.name} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

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

                logger.success(f"Total of {len(stock_objs)} daily data for {stock.name} committed")
                # TODO async
                sleep(TIME_SLEEP_SECS)

        else:
            logger.error(f"Market {market_name} not in database")


def refresh_stock_daily(engine: Engine, today: Optional[date] = None) -> None:
    if today is None:
        today = previous_trade_day(date.today(), inclusive=True)

    if not is_stock_market_open(today):
        logger.error(f"Stock market is not open on {today.isoformat()}")
        return


    with Session(engine) as session:
        df = pull_stock_daily()

        db_stock_codes = {row[0] for row in session.execute(select(Stock.code)).fetchall()}
        data_stock_codes = set(df['code'])
        stock_codes_to_handle = data_stock_codes.difference(db_stock_codes)
        for code in stock_codes_to_handle:
            # TODO 
            # dynamic handle
            # query stock info change
            logger.warning(f"stock {code} returned not availabe in database")
        df = df[~df['code'].isin(stock_codes_to_handle)]
        df['trade_day'] = today

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


def refresh_collection_daily(engine: Engine, collection_type: CollectionType, today: Optional[date] = None) -> None:
    if today is None:
        today = previous_trade_day(date.today(), inclusive=True)

    if not is_stock_market_open(today):
        logger.error(f"Stock market is not open on {today.isoformat()}")
        return

    with Session(engine) as session:
        df = pull_collection_daily(collection_type)

        db_collection_codes = {row[0] for row in session.execute(select(Collection.code)).fetchall()}
        data_collection_codes = set(df['code'])
        collection_codes_to_handle = data_collection_codes.difference(db_collection_codes)
        for code in collection_codes_to_handle:
            # TODO 
            # dynamic handle
            # query stock info change
            logger.warning(f"collection {code} returned not availabe in database")
        df = df[~df['code'].isin(collection_codes_to_handle)]
        df['trade_day'] = today

        try:
            if engine.dialect.name == 'postgresql':
                # upsert statement
                data = df.to_dict(orient='records')
                stmt = pg_insert(CollectionDaily).values(data)
                update_dict = {
                    'price': stmt.excluded.price,
                    'change': stmt.excluded.change,
                    'change_rate': stmt.excluded.change_rate,
                    'capital': stmt.excluded.capital,
                    'turnover_rate': stmt.excluded.turnover_rate,
                    'gainer_count': stmt.excluded.gainer_count,
                    'loser_count': stmt.excluded.loser_count,
                    'top_gainer': stmt.excluded.top_gainer,
                    'top_gain': stmt.excluded.top_gain,
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=['code', 'trade_day'],
                    set_=update_dict
                )
                session.execute(stmt)

            else:
                for _, row in df.iterrows():
                    one_collection = {key: (None if isna(value) else value) for key, value in row.to_dict().items()}
                    stock = CollectionDaily(
                        id=one_collection['id'],
                        trade_day=today,
                        price=one_collection['price'],
                        change=one_collection['change'],
                        change_rate=one_collection['change_rate'],
                        capital=one_collection['capital'],
                        turnover_rate=one_collection['turnover_rate'],
                        gainer_count=one_collection['gainer_count'],
                        loser_count=one_collection['loser_count'],
                        top_gainer=one_collection['top_gainer'],
                        top_gain=one_collection['top_gain'],
                    )
                    session.merge(stock)

            session.commit()
            logger.info(f"Total of {len(df)} daily data committed for {today.isoformat()}")

        except Exception as e:
            session.rollback()
            logger.error(f"Error in committing daily data for {today.isoformat()}: {e}")


if __name__ == '__main__':
    engine = engine_from_env()

    # for market_name in MARKET_UNSUPPORTED:
    # for market_name in MARKET_SUPPORTED:
    #     load_all_stocks(engine, market_name)

    # load 
    # daily data for the past 2 years
    # or
    # today's data
    # for market_name in MARKET_UNSUPPORTED:
    # for market_name in MARKET_SUPPORTED:
    #     load_all_stock_daily_hist(engine, market_name, start_date=date(2025, 1, 28))

    # stock daily info
    # refresh_stock_daily(engine)

    # collection daily info
    refresh_collection_daily(engine, CollectionType.INDUSTRY_BOARD)
