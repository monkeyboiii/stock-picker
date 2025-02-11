-- auto_fill
SELECT
        s.code AS code,
        s.name AS name,
        sd.trade_day AS latest_trade_day
FROM 
        stock s 
JOIN 
        stock_daily sd ON s.code = sd.code
WHERE sd.trade_day = 
(
        SELECT trade_day
        FROM stock_daily
        WHERE code = s.code
        ORDER BY trade_day DESC
        LIMIT 1
);