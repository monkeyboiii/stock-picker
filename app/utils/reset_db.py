import os
import csv
from typing import Optional

from loguru import logger
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.schema import CreateTable, DropTable

from app.constant.confirm import confirms_execution
from app.db.engine import engine_from_env
from app.db.models import Market, MetadataBase


MARKET_CSV_FILE = os.path.join(os.path.dirname(__file__), "../constant/", 'market.csv')


def reset_table_content(
    engine: Engine, 
    dryrun: Optional[bool] = False,
    reset: Optional[bool] = False,
    yes: Optional[bool] = False,
):

    if reset:
        if dryrun:
            for table in MetadataBase.metadata.tables.values():
                logger.info(DropTable(table).compile(dialect=engine.dialect))
        else:
            confirms_execution(
                f"Resetting tables in {engine.url.database} at {engine.url.host}",
                defaultY=False,
                yes=yes
            )
            MetadataBase.metadata.drop_all(engine)

    with Session(engine) as session:
        if dryrun:
            for table in MetadataBase.metadata.tables.values():
                logger.info(CreateTable(table).compile(dialect=engine.dialect))
        
        else:    
            # equiv. to
            # CREATE TABLE :name IF NOT EXISTS
            MetadataBase.metadata.create_all(engine)

        # preload market info
        with open(MARKET_CSV_FILE, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                market = Market(**row)
                session.add(market)
            
            session.commit() if not dryrun else session.rollback()


if __name__ == '__main__':
    reset_table_content(
        engine_from_env(echo=True), 
        dryrun=False,
        reset=True, 
        yes=False,
    )
