from datetime import date
from typing import List, Optional

from loguru import logger
from pandas import DataFrame
from sqlalchemy import Engine, delete, insert
from sqlalchemy.orm import Session

from app.constant.schedule import previous_trade_day
from app.db.models import FeedDaily
from app.profile.tracer import trace_elapsed


@trace_elapsed()
def refresh_feed_daily_table(
    engine: Engine,
    fds: List[FeedDaily],
    trade_day: Optional[date] = None,
) -> DataFrame:
    if trade_day is None:
        trade_day = previous_trade_day(trade_day, inclusive=True)

    df = FeedDaily.to_dataframe(fds)

    assert (df["trade_day"] == trade_day).all(), (
        f"Not all trade_day is {trade_day.isoformat()} in df"
    )
    data = df.to_dict(orient="records")

    with Session(engine) as session:
        delete_stmt = delete(FeedDaily).where(FeedDaily.trade_day == trade_day)
        insert_stmt = insert(FeedDaily).values(data)

        session.execute(delete_stmt)
        session.execute(insert_stmt)

        session.commit()
        logger.success(
            f"A total {df.shape[0]} of matching records committed into feed_daily"
        )

    return df


if __name__ == "__main__":
    from app.db.engine import engine_from_env
    from app.filter.tail_scraper import filter_desired

    trade_day = date(2025, 3, 4)
    engine = engine_from_env()
    fds = filter_desired(
        engine=engine,
        trade_day=trade_day,
    )
    df = refresh_feed_daily_table(
        engine=engine,
        fds=fds,
        trade_day=trade_day,
    )
    print(df.head(5))
