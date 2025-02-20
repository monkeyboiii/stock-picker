from datetime import date
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from loguru import logger
from pandas import DataFrame
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from app.constant.exchange import *
from app.constant.schedule import previous_trade_day
from app.db.models import Market, Stock
from app.filter.tail_scraper import filter_desired


load_dotenv(override=True)


TDX_PATH = Path(f"{os.environ.get('TDX_PATH', os.getcwd())}")


def prefix_market_number(code: str, market_name_short: str) -> str:
    if market_name_short == SEX_SHENZHEN:
        return f"0{code}"
    elif market_name_short == SEX_SHANGHAI:
        return f"1{code}"
    elif market_name_short == SEX_BEIJING:
        return f"2{code}"
    else:
        logger.error(f"market {market_name_short} not supported")
        return code


def add_to_tdx_path(engine: Engine, df: DataFrame, trade_day: Optional[date] = None) -> None:
    if trade_day is None:
        trade_day = previous_trade_day(date.today())

    with Session(engine) as session:
        market_query = select(
            Stock.code, Stock.name, Market.name_short
        ).join(Market, Stock.market_id == Market.id).where(
            Stock.code.in_(df['code'])
        )

        result = session.execute(market_query)

        df = DataFrame(result.fetchall(), columns=result.columns)
        df.loc[:, 'code'] = df.apply(
            lambda x: prefix_market_number(x['code'], x['name_short'])
        )
        df = df[['code', 'name']]
        df.to_csv(Path.joinpath(TDX_PATH, f"选股-{trade_day}.csv"), index=False, header=False)



if __name__ == '__main__':
    from app.db.engine import engine_from_env

    engine = engine_from_env()
    trade_day = date(2025, 2, 10)
    df = filter_desired(engine, trade_day, dryrun=True)
    add_to_tdx_path(engine, df, trade_day)