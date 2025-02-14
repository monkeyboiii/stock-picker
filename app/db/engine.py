import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine


load_dotenv(override=True)


def engine_from_env(**kwargs) -> Engine:

    url = URL.create(
        drivername= os.getenv("DB_DRIVER")          or 'postgresql',
        username=   os.getenv("POSTGRES_USERNAME")  or 'postgres',
        password=   os.getenv("POSTGRES_PASSWORD")  or 'postgres',
        host=       os.getenv("POSTGRES_HOST")      or 'localhost',
        port=   int(os.getenv("POSTGRES_PORT")      or '5432'),
        database=   os.getenv("POSTGRES_DATABASE")
    )

    if url.drivername != 'postgresql':
        raise Exception("Only support postgresql atm")

    return create_engine(url, **kwargs)
