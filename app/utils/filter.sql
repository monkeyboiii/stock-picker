--
-- optimized
-- for dbms that support lateral
WITH static_filtering AS 
(
        -- static_filtering_cte
        SELECT
                s.code,
                s.name,
                sd.trade_day,
                sd.close,
                sd.volume,
                sd.turnover_rate
        FROM stock_daily sd
        JOIN stock s ON sd.code = s.code
        WHERE
                -- T0
                sd.trade_day = '2025-02-07' AND

                -- T2
                sd.quantity_relative_ratio >= 1 AND
                
                -- T3
                sd.turnover_rate > 5.0 AND

                -- T4
                sd.circulation_capital BETWEEN 2_0000_0000 AND 200_0000_0000 AND
                
                -- T6
                s.name NOT LIKE '%ST%' AND
                s.name NOT LIKE '%*%' AND
                
                -- T7
                sd.close > sd.ma_250 AND

                -- T8
                sd.close > sd.open
)
SELECT 
        sf.code,
        sf.name,
        sf.trade_day,
        sf.close,
        prev.close AS previous_close,
        ROUND(100.0 * (sf.close / prev.close - 1), 3) AS increase_ratio,
        sf.volume,
        vol_prev.vol_prev_day as volume_previous_day,
        ROUND(vol_prev.vol_ma5) AS volume_ma_5
FROM static_filtering sf

LEFT JOIN LATERAL 
(
        -- prev_subq
        SELECT close
        FROM stock_daily
        WHERE code = sf.code
        AND trade_day < sf.trade_day
        ORDER BY trade_day DESC
        LIMIT 1
) prev ON true

LEFT JOIN LATERAL 
(
        -- vol_prev_subq
        SELECT 
       (
                -- vol_ma5_expr
                SELECT avg(innermost.volume) AS avg_vol
                FROM 
                (
                        -- innermost
                        SELECT volume AS volume
                        FROM stock_daily
                        WHERE code = sf.code
                        ORDER BY trade_day DESC
                        LIMIT 5
                ) AS innermost
       ) AS vol_ma5,
       (        
                -- vol_prev_day_expr
                SELECT volume
                FROM stock_daily
                WHERE code = sf.code
                AND trade_day < sf.trade_day
                ORDER BY trade_day DESC
                LIMIT 1
       ) AS vol_prev_day
       FROM stock_daily
       WHERE code = sf.code AND trade_day = '2025-02-07'
) vol_prev ON true

-- T1
WHERE   ROUND(100.0 * (sf.close / prev.close - 1), 3) BETWEEN 3 AND 5

-- T5
AND     sf.volume > vol_prev.vol_ma5
AND     vol_prev.vol_prev_day < vol_prev.vol_ma5

-- Tx
ORDER BY sf.turnover_rate DESC;



--
-- simple
-- for most dbms
-- TODO: implement