from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv
from db.models import MetadataBase
import os


load_dotenv()


url = URL.create(
    drivername=os.getenv("DB_DRIVER") or 'postgresql',
    username=os.getenv("POSTGRES_USERNAME") or 'postgres',
    password=os.getenv("POSTGRES_PASSWORD") or 'postgres',
    host=os.getenv("POSTGRES_HOST") or 'localhost',
    port=int(os.getenv("POSTGRES_PORT") or '5432'),
    database=os.getenv("POSTGRES_DATABASE")
)

engine = create_engine(url)


# Assuming your models inherit from Base
MetadataBase.metadata.create_all(engine)