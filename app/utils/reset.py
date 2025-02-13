from typing import Optional

from loguru import logger
from sqlalchemy.engine import Engine
from sqlalchemy.schema import CreateTable, DropTable

from app.constant.confirm import confirms_execution
from app.db.engine import engine_from_env
from app.db.models import MetadataBase


def reset_db_content(
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
                defaultYes=False,
                yes=yes,
            )
            MetadataBase.metadata.drop_all(engine)

    if dryrun:
        for table in MetadataBase.metadata.tables.values():
            logger.info(CreateTable(table).compile(dialect=engine.dialect))
    else:
        # equiv. to
        # CREATE TABLE :name IF NOT EXISTS
        logger.info(f'Creating {len(MetadataBase.metadata.tables.keys())} tables in {engine.url.database} at {engine.url.host}')
        MetadataBase.metadata.create_all(engine)


def reset_table_content(
    engine: Engine, 
    dryrun: Optional[bool] = False,
    reset: Optional[bool] = False,
    yes: Optional[bool] = False,
):
    pass


if __name__ == '__main__':
    reset_db_content(
        engine_from_env(echo=True), 
        dryrun=False,
        reset=True, 
        yes=False,
    )
