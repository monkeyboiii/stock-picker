"""
Loading is for static/semi-static data like stock/market/collection
"""

import os
import csv
from typing import List, Optional
from time import sleep

from loguru import logger
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.data.ak import pull_collections, pull_stocks, pull_stocks_in_collection
from app.constant.exchange import *
from app.constant.misc import *
from app.constant.collection import *
from app.db.engine import engine_from_env
from app.db.models import Collection, Market, Stock
from app.profile.tracer import trace_elapsed


MARKET_CSV_FILE = os.path.join(os.path.dirname(__file__), "../constant/", 'market.csv')


def load_market(engine: Engine) -> None:
     with Session(engine) as session:

        # preloaded market info
        with open(MARKET_CSV_FILE, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                market = Market(**row)
                session.add(market)
            
            session.commit()
            logger.success('market table loaded')
        


def load_all_stocks(engine: Engine, market_name: str) -> None:
    if market_name not in MARKET_SUPPORTED:
        raise ValueError(f"exchange {market_name} not supported")


    with Session(engine) as session:
        market = session.execute(
            select(Market).where(Market.name_short == market_name)
        ).scalar_one_or_none()

        if market is not None:
            logger.info(f"Getting stocks info for {market_name}")

            df = pull_stocks(market_name)
            for code, name in zip(df['code'], df['name']):
                if code in BAD_STOCKS:
                    logger.warning(f"Bad stock {name} filtered out")
                    continue
                stock = Stock(
                    code=code,
                    name=name,
                    market_id=market.id
                )
                session.add(stock)

            session.commit()
            logger.success(f"Total of {len(df)} stocks committed into {market_name}")

        else:
            logger.warning(f"Market {market_name} not in database")



def load_collection(engine: Engine, collection_type: CollectionType) -> None:
    if collection_type not in CollectionType:
        raise ValueError(f"CollectionType {collection_type} not supported")

    with Session(engine) as session:
        df = pull_collections(cType=collection_type)

        for code, name in zip(df['code'], df['name']):
            collection = Collection(
                code=code,
                name=name,
                type=collection_type,
            )
            session.add(collection)

        session.commit()


def load_default_collections(engine: Engine) -> List[CollectionType]:
    default_collection_types = [
        CollectionType.INDUSTRY_BOARD
    ]

    for cType in default_collection_types:
        load_collection(engine, cType)

    return default_collection_types

def load_collection_stock_relation(engine: Engine, collection_type: CollectionType) -> None:
    if collection_type not in CollectionType:
        raise ValueError(f"CollectionType {collection_type} not supported")
    
    with Session(engine) as session:
        count = 0

        collections = session.execute(
            select(Collection).where(Collection.type == collection_type)
        )

        for collection in collections:
            cName = collection[0].name

            df = pull_stocks_in_collection(cType=collection_type, symbol=cName)
            sleep(TIME_SLEEP_SECS)
            
            for code, name in zip(df['code'], df['name']):
                stock = session.query(Stock).filter_by(code=code).first()
                if stock:
                    count += 1
                    stock.collections.append(collection[0])
                else:
                    logger.warning(f"Stock {name} not found in database yet in {cName}")
            
            session.flush()
            logger.info(f"Linked {len(df)} stocks for collection {cName}")

        session.commit()
        logger.success(f"Total of {count} stock-collections relations linked and committed")



@trace_elapsed(unit='s')
def load_by_level(engine: Engine, level: Optional[int] = 0) -> None:
    if not level:
        logger.info("Load level 0: none")
        return
    
    if level >= 1:
        load_market(engine)
    if level >= 2:
        for market_name in MARKET_SUPPORTED:
            load_all_stocks(engine, market_name)
    if level >= 3:
        dcts = load_default_collections(engine)
        for cType in dcts:
            load_collection_stock_relation(engine, cType)


if __name__ == '__main__':
    engine = engine_from_env()
    
    # load_market(engine_from_env(echo=True))

    # load_all_stocks(engine)

    # load_default_collections()

    load_by_level(engine, 3)