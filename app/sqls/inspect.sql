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