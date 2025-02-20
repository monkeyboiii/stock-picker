from datetime import date
from typing import Optional

from loguru import logger
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session

from app.constant.schedule import previous_trade_day
from app.profile.tracer import trace_elapsed


MV_STOCK_DAILY = 'mv_stock_daily'


CREATE_MV_FUNCTION_SQL = f"""
CREATE OR REPLACE FUNCTION create_mv_with_trade_day(input_trade_day DATE) 
RETURNS void AS
$$
BEGIN
        DROP MATERIALIZED VIEW IF EXISTS {MV_STOCK_DAILY};

        execute format('
CREATE MATERIALIZED VIEW {MV_STOCK_DAILY} AS
SELECT
        sd.code,
        sd.trade_day,                                           -- last trade day
        sd.close,                                               -- previous close
        sd.volume,                                              -- previous volume
        ROUND(ma250_subq.ma250, 3)      AS ma250,               -- 250-day moving average of close
        ma250_subq.close                AS prev_250_close,      -- close 250 days ago
        volume_ma5_subq.volume_ma5      AS ma5_volume,          -- 5-day moving average of volume
        volume_ma5_subq.volume          AS prev_5_volume        -- volume 5 days ago
FROM stock_daily sd

JOIN LATERAL 
(
        -- ma250_subq
        SELECT 
        (
                -- ma250_expr
                SELECT AVG(ma250_innermost.close)
                FROM 
                (
                        -- ma250_innermost
                        SELECT close
                        FROM stock_daily
                        WHERE code = sd.code
                        AND trade_day <= sd.trade_day
                        ORDER BY trade_day DESC
                        LIMIT 250
                ) AS ma250_innermost
        ) AS ma250,
        (
                SELECT COUNT(ma250_innermost.close)
                FROM 
                (
                        -- ma250_innermost
                        SELECT close
                        FROM stock_daily
                        WHERE code = sd.code
                        AND trade_day <= sd.trade_day
                        ORDER BY trade_day DESC
                        LIMIT 250
                ) AS ma250_innermost
        ) AS row_count,
        (
                -- ma250_expr
                SELECT close
                FROM 
                (
                        -- ma250_innermost
                        SELECT 
                                close,
                                trade_day
                        FROM stock_daily
                        WHERE code = sd.code
                        AND trade_day <= sd.trade_day
                        ORDER BY trade_day DESC
                        LIMIT 250
                ) AS ma250_innermost
                ORDER BY trade_day ASC 
                LIMIT 1
        ) AS close
        FROM stock_daily
        WHERE code = sd.code AND trade_day = %L
) ma250_subq ON true

JOIN LATERAL 
(
        -- volume_ma5_subq
        SELECT 
        (
                -- vol_ma5_expr
                SELECT AVG(volume_ma5_innermost.volume)
                FROM 
                (
                        -- volume_ma5_innermost
                        SELECT volume
                        FROM stock_daily
                        WHERE code = sd.code
                        AND trade_day <= sd.trade_day
                        ORDER BY trade_day DESC
                        LIMIT 5
                ) AS volume_ma5_innermost
        ) AS volume_ma5,
        (
                -- vol_ma5_volume_expr
                SELECT volume
                FROM 
                (
                        -- volume_ma5_innermost
                        SELECT volume
                        FROM stock_daily
                        WHERE code = sd.code
                        AND trade_day <= sd.trade_day
                        ORDER BY trade_day DESC
                        LIMIT 5
                ) AS volume_ma5_innermost
                ORDER BY trade_day ASC
                LIMIT 1
        ) AS volume
        FROM stock_daily
        WHERE code = sd.code AND trade_day = %L
) volume_ma5_subq ON true

WHERE 
        sd.trade_day = %L AND 
        ma250_subq.row_count = 250;', input_trade_day, input_trade_day, input_trade_day);
END;
$$ LANGUAGE plpgsql;
"""


DAILY_RECREATE_MV_SQL = "SELECT create_mv_with_trade_day('{}');"


CHECK_MV_EXISTS_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM pg_matviews
    WHERE matviewname = :mv_name
);
"""


@trace_elapsed()
def init_db_mv(engine: Engine) -> None:
    with Session(engine) as session:
        session.execute(text(CREATE_MV_FUNCTION_SQL))
        session.commit()
        logger.success("Materialized view created successfully")


@trace_elapsed()
def daily_recreate_mv(engine: Engine, trade_day: Optional[date] = None) -> None:
    if trade_day is None:
        trade_day = previous_trade_day(date.today())
    
    with Session(engine) as session:
        session.execute(text(DAILY_RECREATE_MV_SQL.format(trade_day.isoformat())))
        session.commit()
        logger.success("Materialized view recreated successfully")


@trace_elapsed()
def check_mv_exists(engine: Engine, mv_name: Optional[str] = MV_STOCK_DAILY) -> bool:
    with Session(engine) as session:
        result = session.execute(text(CHECK_MV_EXISTS_SQL), {'mv_name': mv_name})
        return result.scalar()


if __name__ == '__main__':
    from app.db.engine import engine_from_env

    engine = engine_from_env()
    if engine.dialect.name != 'postgresql':
        raise ValueError("Only support postgresql")
    
    if not check_mv_exists(engine):
        init_db_mv(engine)
    daily_recreate_mv(engine, trade_day=date(2025, 2, 7))
    assert check_mv_exists(engine)