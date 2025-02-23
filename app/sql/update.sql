--
-- optimized
-- for dbms that support lateral
SELECT 
       s.code,
       s.name,
       latest.ma250 AS ma250
FROM stock s
JOIN stock_daily sd ON s.code = sd.code
JOIN LATERAL 
(
       SELECT 
       (
                SELECT AVG(innermost.close)
                FROM 
                (
                        SELECT close
                        FROM stock_daily 
                        WHERE code = s.code
                        AND trade_day <= '2025-02-21'
                        ORDER BY trade_day DESC 
                        LIMIT 250
                ) AS innermost
       )
       AS ma250,
       (
                SELECT COUNT(innermost.close)
                FROM 
                (
                        SELECT close
                        FROM stock_daily
                        WHERE code = s.code
                        AND trade_day <= '2025-02-21'
                        ORDER BY trade_day DESC
                        LIMIT 2z50
                ) AS innermost
       ) 
       AS row_count
       FROM stock_daily 
       WHERE code = s.code AND trade_day = '2025-02-21'
) AS latest ON true
WHERE 
        sd.trade_day = '2025-02-21' AND
        latest.row_count = 250;


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