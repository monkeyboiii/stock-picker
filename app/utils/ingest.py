from time import sleep
from datetime import date
from decimal import Decimal

from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.ak.data import *
from app.constant.exchange import *
from app.db.models import Market, Stock, StockDaily


def load_all_stocks(engine: Engine, market_name: str):
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



def load_all_stock_daily_hist(engine: Engine, market_name: str):
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
                end_date = date.today()
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
                        trade_day=row['日期'],
                        open=Decimal(format(row['开盘'], '.3f')),
                        high=Decimal(format(row['最高'], '.3f')),
                        low=Decimal(format(row['最低'], '.3f')),
                        close=Decimal(format(row['收盘'], '.3f')),
                        volume=int(row['成交量']),
                        turnover=int(row['成交额'])
                    )
                    for _, row in df.iterrows()
                ]

                session.add_all(stock_objs)
                session.commit()
                logger.info(f"Total of {len(stock_objs)} daily data for {stock.name} committed")
                sleep(1)


            #     logger.warning(f"No stocks data in {market_name}")    
        else:
            logger.error(f"Market {market_name} not in database")



# TODO testing required
def refresh_stock_daily(engine: Engine):
    with Session(engine) as session:

        today = date.today()
        df = pull_stock_daily()

        if engine.dialect.name == 'postgresql':
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
        else:
            # Assuming you have a pandas DataFrame 'df' with columns: ticker, price, timestamp
            for _, row in df.iterrows():
                # Convert the row to a dictionary; make sure the keys match your model
                one_stock = row.to_dict()
                price_obj = StockDaily(
                    code=one_stock.code,
                    trade_day=one_stock['日期'],
                    open=Decimal(format(one_stock['开盘'], '.3f')),
                    high=Decimal(format(one_stock['最高'], '.3f')),
                    low=Decimal(format(one_stock['最低'], '.3f')),
                    close=Decimal(format(one_stock['收盘'], '.3f')),
                    volume=int(one_stock['成交量']),
                    turnover=int(one_stock['成交额'])
                )
                session.merge(one_stock)



if __name__ == '__main__':
    from dotenv import load_dotenv
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL
    import os


    load_dotenv()

    url = URL.create(
        drivername  =       os.getenv("DB_DRIVER")          or 'postgresql',
        username    =       os.getenv("POSTGRES_USERNAME")  or 'postgres',
        password    =       os.getenv("POSTGRES_PASSWORD")  or 'postgres',
        host        =       os.getenv("POSTGRES_HOST")      or 'localhost',
        port        =   int(os.getenv("POSTGRES_PORT")      or '5432'),
        database    =       os.getenv("POSTGRES_DATABASE")
    )
    
    supported_markets = [SEX_SHANGHAI, SEX_SHENZHEN, SEX_BEIJING]
    unsupported_markets = [SEX_HONGKONG]
    
    # for market_name in unsupported_markets:
    # for market_name in supported_markets:
    #     load_all_stocks(create_engine(url), market_name)

    # load 
    # daily data for the past 2 years
    # or
    # today's data
    # for market_name in unsupported_markets:
    for market_name in supported_markets:
        load_all_stock_daily_hist(create_engine(url), market_name)
