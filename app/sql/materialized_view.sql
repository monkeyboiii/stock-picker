SELECT EXISTS (
        SELECT 1
        FROM pg_matviews
        WHERE matviewname = 'mv_stock_daily_2025_02_28'
);



-- inner
CREATE MATERIALIZED VIEW mv_stock_daily_2025_02_28 AS
SELECT
        sd.code,
        sd.trade_day,                                           -- last trade day
        sd.close,                                               -- previous close
        sd.volume,                                              -- previous volume
        ma250_subq.ma250                AS ma250,               -- 250-day moving average of close
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
                SELECT AVG(close)
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
                SELECT COUNT(close)
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
        WHERE code = sd.code AND trade_day = sd.trade_day
) ma250_subq ON true

JOIN LATERAL
(
        -- volume_ma5_subq
        SELECT 
        (
                -- vol_ma5_expr
                SELECT AVG(volume)
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
                        SELECT 
                                volume,
                                trade_day
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
        WHERE code = sd.code AND trade_day = sd.trade_day
) volume_ma5_subq ON true

WHERE 
        sd.trade_day = '2025-02-28' AND 
        ma250_subq.row_count = 250;



CREATE OR REPLACE FUNCTION create_mv_with_trade_day(input_trade_day DATE) 
RETURNS boolean AS
$$
DECLARE
  exists_result boolean;
BEGIN
        EXECUTE format(
                'DROP MATERIALIZED VIEW IF EXISTS %s;', 
                'mv_stock_daily_' || replace(input_trade_day::text, '-', '_')
        );


        EXECUTE format('
CREATE MATERIALIZED VIEW %s AS
SELECT
        sd.code,
        sd.trade_day,                                           -- last trade day
        sd.close,                                               -- previous close
        sd.volume,                                              -- previous volume
        ma250_subq.ma250                AS ma250,               -- 250-day moving average of close
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
                SELECT AVG(close)
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
                SELECT COUNT(close)
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
        WHERE code = sd.code AND trade_day = sd.trade_day
) ma250_subq ON true

JOIN LATERAL
(
        -- volume_ma5_subq
        SELECT 
        (
                -- vol_ma5_expr
                SELECT AVG(volume)
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
                        SELECT 
                                volume,
                                trade_day
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
        WHERE code = sd.code AND trade_day = sd.trade_day
) volume_ma5_subq ON true

WHERE 
        sd.trade_day = %L AND 
        ma250_subq.row_count = 250;', 

        -- %s
        'mv_stock_daily_' || replace(input_trade_day::text, '-', '_'),

        -- %L
        input_trade_day
);


        SELECT EXISTS (
                SELECT 1
                FROM pg_matviews
                WHERE matviewname = 'mv_stock_daily_' || replace(input_trade_day::text, '-', '_')
        ) INTO exists_result;


        RETURN exists_result;
END;
$$ LANGUAGE plpgsql;
