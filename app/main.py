from datetime import date

from db.engine import engine_from_env
from db.ingest import refresh_stock_daily
from utils.update import calculate_ma250
from utils.filter import filter_out_desired


def main():
    trade_day = date(2025, 2, 7)
    engine = engine_from_env()
    
    refresh_stock_daily(engine, trade_day)
    calculate_ma250(engine, trade_day)

    df = filter_out_desired(engine, trade_day)

    print(df)

    df.to_csv(f'report-{trade_day}.csv')



if __name__ == "__main__":
    main()
