CREATE TABLE market (
        id SERIAL NOT NULL, 
        name VARCHAR NOT NULL, 
        name_short VARCHAR, 
        country VARCHAR, 
        open TIME WITHOUT TIME ZONE, 
        break_start TIME WITHOUT TIME ZONE, 
        break_end TIME WITHOUT TIME ZONE, 
        close TIME WITHOUT TIME ZONE, 
        currency VARCHAR, 
        PRIMARY KEY (id)
)


CREATE TABLE collection (
        code VARCHAR(30) NOT NULL, 
        name VARCHAR NOT NULL, 
        type collectiontype NOT NULL, 
        PRIMARY KEY (code)
)


CREATE TABLE stock (
        code VARCHAR(10) NOT NULL, 
        name VARCHAR(50) NOT NULL, 
        market_id INTEGER NOT NULL, 
        PRIMARY KEY (code), 
        UNIQUE (name), 
        FOREIGN KEY(market_id) REFERENCES market (id)
)


CREATE TABLE relation_collection_stock (
        collection_code VARCHAR(30) NOT NULL, 
        stock_code VARCHAR(10) NOT NULL, 
        PRIMARY KEY (collection_code, stock_code), 
        FOREIGN KEY(collection_code) REFERENCES collection (code), 
        FOREIGN KEY(stock_code) REFERENCES stock (code)
)


CREATE TABLE collection_daily (
        code VARCHAR(30) NOT NULL, 
        trade_day DATE NOT NULL, 
        last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
        price NUMERIC(10, 3), 
        change NUMERIC(10, 3), 
        change_rate FLOAT, 
        capital BIGINT, 
        turnover_rate FLOAT, 
        gainer_count INTEGER, 
        loser_count INTEGER, 
        top_gainer VARCHAR(50), 
        top_gain FLOAT, 
        PRIMARY KEY (code, trade_day), 
        FOREIGN KEY(code) REFERENCES collection (code), 
        FOREIGN KEY(top_gainer) REFERENCES stock (name)
)


CREATE TABLE stock_daily (
        code VARCHAR(10) NOT NULL, 
        trade_day DATE NOT NULL, 
        last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
        open NUMERIC(10, 3), 
        high NUMERIC(10, 3), 
        low NUMERIC(10, 3), 
        close NUMERIC(10, 3), 
        volume BIGINT, 
        turnover BIGINT, 
        capital BIGINT, 
        circulation_capital BIGINT, 
        quantity_relative_ratio FLOAT, 
        turnover_rate FLOAT, 
        ma_250 FLOAT, 
        PRIMARY KEY (code, trade_day), 
        FOREIGN KEY(code) REFERENCES stock (code)
)


CREATE TABLE feed_daily (
        code VARCHAR(10) NOT NULL, 
        trade_day DATE NOT NULL, 
        filter_id INTEGER NOT NULL, 
        last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
        name VARCHAR NOT NULL, 
        volume BIGINT NOT NULL, 
        close NUMERIC(10, 3) NOT NULL, 
        gain FLOAT NOT NULL, 
        previous_close NUMERIC(10, 3) NOT NULL, 
        previous_volume BIGINT NOT NULL, 
        ma_5_volume BIGINT NOT NULL, 
        PRIMARY KEY (code, trade_day, filter_id), 
        FOREIGN KEY(code) REFERENCES stock (code)
)