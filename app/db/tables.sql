CREATE TABLE
    "stock" (
        "code" varchar(10) PRIMARY KEY,
        "name" text,
        "market_id" integer,
        FOREIGN KEY (market_id) REFERENCES market (id),
    );


CREATE TABLE
    "market" (
        "id" integer PRIMARY KEY,
        -- 
        "name" text,
        "name_short" varchar(10),
        "country" varchar(10),
        -- time
        "open" time,
        "break_start" time,
        "break_end" time,
        "close" time,
        -- beijing time
        -- "open_beijing" time,
        -- "close_beijing" time,
        -- misc
        "currency" varchar,
        -- optimize to separate table
        -- "timezone" varchar,
        -- "timezone_offset" integer,
        -- "dst_start" date,
        -- "dst_stop" date,
    );


CREATE TABLE
    "stock_daily" (
        "code" varchar(10),
        "trade_day" date NOT NULL,
        "open" NUMERIC(10, 3),
        "high" NUMERIC(10, 3),
        "low" NUMERIC(10, 3),
        "close" NUMERIC(10, 3),
        "volume" bigint,
        "capital" bigint,
        "quantity_relative_ratio" real,
        "turnover_rate" real,
        "ma_250" real,
        -- close/open C/O
        FOREIGN KEY (code) REFERENCES stock (code),
    );


CREATE TABLE stock_daily_dupe (LIKE stock_daily INCLUDING ALL);


INSERT INTO stock_daily_dupe SELECT * FROM stock_daily WHERE trade_day = '2025-02-06';