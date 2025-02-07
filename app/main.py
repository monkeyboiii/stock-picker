from db.engine import engine_from_env
from utils.ingest import refresh_stock_daily


def main():
    refresh_stock_daily(engine_from_env())



if __name__ == "__main__":
    main()
