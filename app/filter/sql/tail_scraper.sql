--
-- for dbms that support lateral
WITH static_filtering AS 
(
        -- static_filtering_cte
        SELECT
                sd.trade_day,
                s.code,
                s.name,
                sd.close,
                sd.volume,
                sd.ma_250
        FROM stock s
        JOIN stock_daily sd ON s.code = sd.code
        WHERE
                -- T0
                sd.trade_day = '2025-03-10' AND

                -- T2
                sd.quantity_relative_ratio >= 1.0 AND
                
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
        sf.code                                 AS code,
        sf.name                                 AS name,
        c.name                                  AS collection_name,
        cd.change_rate                          AS collection_performance,
        sf.close                                AS close,
        -- sf.ma_250                               AS ma250,
        prev.close                              AS previous_close,
        100.0 * (sf.close / prev.close - 1)     AS gain,
        -- prev_volume.ma5_volume                  AS ma5_volume,
        prev_volume.volume                      AS previous_volume,
        sf.volume                               AS volume
FROM static_filtering sf

JOIN LATERAL 
(
        -- prev_subq
        SELECT close
        FROM stock_daily
        WHERE code = sf.code
        AND trade_day < sf.trade_day
        ORDER BY trade_day DESC
        LIMIT 1
) prev ON true

JOIN LATERAL 
(
        -- prev_volume_subq
        SELECT 
       (
                -- prev_volume_avg_expr
                SELECT AVG(volume)
                FROM 
                (
                        -- prev_volume_innermost
                        SELECT volume
                        FROM stock_daily
                        WHERE code = sf.code
                        AND trade_day <= sf.trade_day
                        ORDER BY trade_day DESC
                        LIMIT 5
                ) AS prev_volume_innermost
       ) AS ma5_volume,
       (        
                -- prev_volume_volume_expr
                SELECT volume
                FROM stock_daily
                WHERE code = sf.code
                AND trade_day < sf.trade_day
                ORDER BY trade_day DESC
                LIMIT 1
       ) AS volume
       FROM stock_daily
       WHERE code = sf.code AND trade_day = sf.trade_day
) prev_volume ON true

JOIN relation_collection_stock  rcs     ON sf.code = rcs.stock_code
JOIN collection                 c       ON c.code = rcs.collection_code
JOIN collection_daily           cd      ON c.code = cd.code AND sf.trade_day = cd.trade_day 

-- T1
WHERE   100.0 * (sf.close / prev.close - 1) BETWEEN 3 AND 5

-- T5
AND     prev_volume.ma5_volume < sf.volume
AND     prev_volume.ma5_volume > prev_volume.volume

-- Tx
ORDER BY collection_performance DESC;



--
-- for dbms that supports materialized view
SELECT 
        sd.code                                                                 AS code,
        s.name                                                                  AS name,
        c.name                                                                  AS collection_name,
        cd.change_rate                                                          AS collection_performance,
        -- prev.ma250 + (sd.close - prev.prev_250_close) / 250                     AS ma_250,
        prev.close                                                              AS previous_close,
        sd.close                                                                AS close,
        100.0 * (sd.close / prev.close - 1)                                     AS gain,
        -- prev.ma5_volume + (sd.volume - prev.prev_5_volume) / 5                  AS ma5_volume,
        prev.volume                                                             AS previous_volume,
        sd.volume                                                               AS volume,
        100.0 * ((1.0 * sd.volume) / (1.0 * prev.volume) - 1)                   AS volume_gain
FROM mv_stock_daily_2025_03_07  prev
JOIN stock_daily                sd      ON prev.code = sd.code
JOIN stock                      s       ON sd.code = s.code
JOIN relation_collection_stock  rcs     ON s.code = rcs.stock_code
JOIN collection                 c       ON c.code = rcs.collection_code
JOIN collection_daily           cd      ON c.code = cd.code AND sd.trade_day = cd.trade_day 
WHERE 
        -- T0
        sd.trade_day = '2025-03-10' AND

        -- T1
        100.0 * (sd.close / prev.close - 1) BETWEEN 3 AND 5 AND
        
        -- T2
        sd.quantity_relative_ratio >= 1.0 AND
        
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
ORDER BY collection_performance DESC;

