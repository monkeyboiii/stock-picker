--
-- optimized
-- for dbms that support lateral
SELECT 
       stock.code, 
       stock.name, 
       round(latest.ma250, 3) AS ma250 
FROM stock 
JOIN LATERAL 
(
       SELECT 
       (
                SELECT avg(innermost.close) AS avg_1 
                FROM 
                (
                        SELECT stock_daily.close AS close 
                        FROM stock_daily 
                        WHERE stock_daily.code = stock.code 
                        ORDER BY stock_daily.trade_day DESC 
                        LIMIT 250
                ) AS innermost
       )
       AS ma250, 
       (
                SELECT count(innermost.close) AS count_1 
                FROM 
                (
                        SELECT stock_daily.close AS close 
                        FROM stock_daily 
                        WHERE stock_daily.code = stock.code 
                        ORDER BY stock_daily.trade_day DESC 
                        LIMIT 250
                ) AS innermost
       ) 
       AS row_count 
       FROM stock_daily 
       WHERE stock_daily.code = stock.code AND stock_daily.trade_day = '2025-02-10'
) AS latest ON true 
WHERE 
        latest.row_count = 250 
ORDER BY 
        stock.code;


--
-- simple
-- for most dbms
-- TODO: correctness on less than 250 data points
SELECT
        sub.code,
        s.name,
        ROUND(sub.ma250, 3) as ma250
FROM 
(
        SELECT code
        AVG(close) OVER 
        (
                PARTITION BY code
                ORDER BY trade_day
                ROWS BETWEEN 249 PRECEDING AND CURRENT ROW
        ) AS ma250,
        ROW_NUMBER() OVER 
        (
                PARTITION BY code 
                ORDER BY trade_day DESC
        ) AS rn
    FROM stock_daily
) sub 
JOIN stock s ON sub.code = s.code
WHERE sub.rn = 1
ORDER BY sub.code;