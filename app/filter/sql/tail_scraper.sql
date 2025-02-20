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
                sd.trade_day = '2025-02-10' AND

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
                sd.low > sd.ma_250 AND

                -- T8
                sd.close > sd.open
)
SELECT 
        sf.code,
        sf.name,
        sf.trade_day,
        sf.close,
        prev.close AS previous_close,
        ROUND(100.0 * (sf.close / prev.close - 1), 3) AS gain,
        sf.volume,
        vol_prev.vol_prev_day as previous_volume,
        ROUND(vol_prev.vol_ma5) AS ma_5_volume
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
                        AND trade_day <= sf.trade_day
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
       WHERE code = sf.code AND trade_day = '2025-02-10'
) vol_prev ON true

-- T1
WHERE   ROUND(100.0 * (sf.close / prev.close - 1), 3) BETWEEN 3 AND 5

-- T5
AND     sf.volume > vol_prev.vol_ma5
AND     vol_prev.vol_prev_day < vol_prev.vol_ma5

-- Tx
ORDER BY sf.turnover_rate DESC;



--
-- simpler
-- for dbms that supports materialized view
SELECT 
        sd.trade_day,
        sd.code,
        s.name,
        c.name                                                                  AS industry,
        cd.change_rate || '%'                                                   AS industry_performance,
        ROUND(prev.ma250 + (sd.close - prev.prev_250_close) / 250, 3)           AS ma_250,
        prev.close                                                              AS previous_close,
        sd.close                                                                AS now_close,
        ROUND(100.0 * (sd.close / prev.close - 1), 3) || '%'                    AS gain,
        ROUND(prev.ma5_volume + (sd.volume - prev.prev_5_volume) / 5)           AS ma5_volume,
        prev.volume                                                             AS prev_volume,
        sd.volume                                                               AS now_volume,
        ROUND(100.0 * ((1.0 * sd.volume) / (1.0 * prev.volume) - 1), 3) || '%'  AS volume_gain
FROM mv_stock_daily             prev
JOIN stock_daily                sd      ON prev.code = sd.code
JOIN stock                      s       ON sd.code = s.code
JOIN relation_collection_stock  rcs     ON s.code = rcs.stock_code
JOIN collection                 c       ON c.code = rcs.collection_code
JOIN collection_daily           cd      ON c.code = cd.code AND sd.trade_day = cd.trade_day 
WHERE 
        -- T0
        sd.trade_day = '2025-02-10' AND

        -- T1
        ROUND(100.0 * (sd.close / prev.close - 1), 3) BETWEEN 3 AND 5 AND
        
        -- T2
        sd.quantity_relative_ratio >= 1 AND
        
        -- T3
        sd.turnover_rate > 5.0 AND

        -- T4
        sd.circulation_capital BETWEEN 2_0000_0000 AND 200_0000_0000 AND

        -- T5
        prev.volume < prev.ma5_volume + (sd.volume - prev.prev_5_volume) / 5 AND
        sd.volume   > prev.ma5_volume + (sd.volume - prev.prev_5_volume) / 5 AND
        
        -- T6
        s.name NOT LIKE '%ST%' AND
        s.name NOT LIKE '%*%' AND
        
        -- T7
        sd.low > prev.ma250 + (sd.close - prev.prev_250_close) / 250 AND

        -- T8
        sd.close > sd.open

-- TX
ORDER BY gain DESC;

