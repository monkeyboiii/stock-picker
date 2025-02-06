import os
import csv

from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine

from app.db.engine import engine_from_env
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
    reset_input = input("Do you want to reset the database? (y/[n]): ")
    reset = True if reset_input.lower() == 'y' else False
    reset_table_content(engine_from_env(), reset=reset)
