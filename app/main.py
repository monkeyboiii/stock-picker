from db.engine import engine_from_env
from utils.ingest import refresh_stock_daily
from utils.update import calculate_ma250


def main():
    engine = engine_from_env()
    
    refresh_stock_daily(engine)
    calculate_ma250(engine)


if __name__ == "__main__":
    main()
