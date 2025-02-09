SELECT
       s.code,
       s.name,
       sd.close,
       LAG(sd.close) OVER 
       (
                PARTITION BY sd.code
                ORDER BY sd.trade_day DESC
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS last_close
FROM stock_daily sd
JOIN stock s on sd.code = s.code
WHERE
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
;

-- TODO: wip do i have to use lateral here too

--
-- optimized
-- for dbms that support lateral
SELECT 
       stock.code, 
       stock.name, 
       
FROM stock 
JOIN LATERAL 
(
       SELECT 
       (
                SELECT avg(inner2.close) AS avg_1 
                FROM 
                (
                        SELECT stock_daily.close AS close 
                        FROM stock_daily 
                        WHERE stock_daily.code = stock.code 
                        ORDER BY stock_daily.trade_day DESC 
                        LIMIT 2
                ) AS inner2
       )
       AS ma250, 
       (
                SELECT count(inner5.close) AS count_1 
                FROM 
                (
                        SELECT stock_daily.close AS close 
                        FROM stock_daily 
                        WHERE stock_daily.code = stock.code 
                        ORDER BY stock_daily.trade_day DESC 
                        LIMIT 2
                ) AS inner5
       ) 
       AS row_count
       FROM stock_daily 
       WHERE stock_daily.code = stock.code AND stock_daily.trade_day = '2025-02-07'
) AS latest ON true 
WHERE 
        latest.row_count = 250 
ORDER BY 
        stock.code;