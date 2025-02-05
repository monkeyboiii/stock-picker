import os
import csv

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.engine import URL, Engine

from app.db.models import Market, MetadataBase


MARKET_CSV_FILE = os.path.join(os.path.dirname(__file__), "../db/", 'market.csv')


def reset_table_content(engine: Engine, reset=False):
    with Session(engine) as session:
        if reset:
            MetadataBase.metadata.drop_all(engine)
        MetadataBase.metadata.create_all(engine)

        # preload market info
        with open(MARKET_CSV_FILE, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                market = Market(**row)
                session.add(market)
            session.commit()


if __name__ == '__main__':
    from dotenv import load_dotenv
    import os

    load_dotenv()

    url = URL.create(
        drivername  =       os.getenv("DB_DRIVER")          or 'postgresql',
        username    =       os.getenv("POSTGRES_USERNAME")  or 'postgres',
        password    =       os.getenv("POSTGRES_PASSWORD")  or 'postgres',
        host        =       os.getenv("POSTGRES_HOST")      or 'localhost',
        port        =   int(os.getenv("POSTGRES_PORT")      or '5432'),
        database    =       os.getenv("POSTGRES_DATABASE")
    )
    
    reset_input = input("Do you want to reset the database? (y/[n]): ")
    reset = True if reset_input.lower() == 'y' else False
    reset_table_content(create_engine(url), reset=reset)
