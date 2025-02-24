-- check market
SELECT * FROM market;

-- check latest trade daily data
SELECT * FROM stock_daily ORDER BY trade_day DESC LIMIT 10;

-- select individual stock
SELECT * FROM stock_daily WHERE code = '688350' ORDER BY trade_day DESC LIMIT 10;

-- count if updated
SELECT COUNT(*) FROM stock_daily WHERE trade_day = '2025-02-10';

-- count if update successful
SELECT COUNT(*) FROM stock_daily WHERE trade_day = '2025-02-10' AND ma_250 IS NOT NULL;

-- industry board
SELECT s.name FROM stock s JOIN relation_collection_stock r ON s.code = r.stock_code JOIN collection c ON r.collection_id = c.id WHERE c.name = '通用设备';

-- materialized view
WITH record_250 AS (
    SELECT close FROM stock_daily 
    WHERE code = '000001' 
    AND trade_day <= '2025-02-07' 
    ORDER BY trade_day DESC
    LIMIT 250
)
SELECT AVG(close), COUNT(close) FROM record_250;