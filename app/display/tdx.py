from datetime import date
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from loguru import logger
from pandas import DataFrame
from sqlalchemy import distinct, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.constant.exchange import (
    SEX_SHANGHAI,
    SEX_SHENZHEN,
    SEX_BEIJING,
)
from app.constant.schedule import previous_trade_day
from app.db.models import FeedDaily, Market, Stock


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


def add_to_tdx_path(engine: Engine, df: Optional[DataFrame] = None, trade_day: Optional[date] = None) -> None:
    if trade_day is None:
        trade_day = previous_trade_day(date.today())

    p = Path.joinpath(TDX_PATH, f"tdx-stock-{trade_day}.blk")

    with Session(engine) as session:
        if df is None:
            market_query = select(
                distinct(Stock.code).label('code'), Stock.name, Market.name_short
            ).join(
                Market, Stock.market_id == Market.id
            ).join(
                FeedDaily, (Stock.code == FeedDaily.code)
            ).where(
                FeedDaily.trade_day == trade_day
            )

            result = session.execute(market_query)
            df = DataFrame(result.all(), columns=result.keys()) # type: ignore

        df.loc[:, 'code'] = df.apply(
            lambda x: prefix_market_number(x['code'], x['name_short']), axis=1
        )
        df = df[['code', 'name']]
        df.to_csv(p, index=False, header=False)
        logger.success(f"Tdx format file of {trade_day.isoformat()} wrote to {p}")


if __name__ == '__main__':
    from app.db.engine import engine_from_env

    engine = engine_from_env()
    trade_day = date(2025, 2, 20)
    
    # df = filter_desired(engine, trade_day, dryrun=True)
    # add_to_tdx_path(engine, df=df, trade_day=trade_day)
    
    add_to_tdx_path(engine, trade_day=trade_day)