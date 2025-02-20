CREATE TABLE feed_daily (
        code VARCHAR(10) NOT NULL, 
        trade_day DATE NOT NULL, 
        filter_id INTEGER NOT NULL, 
        last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
        name VARCHAR NOT NULL, 
        collection_name VARCHAR, 
        collection_performance FLOAT, 
        close NUMERIC(10, 3) NOT NULL, 
        previous_close NUMERIC(10, 3) NOT NULL, 
        gain FLOAT NOT NULL, 
        next_open NUMERIC(10, 3), 
        next_close NUMERIC(10, 3), 
        PRIMARY KEY (code, trade_day, filter_id), 
        FOREIGN KEY(code) REFERENCES stock (code)
)
