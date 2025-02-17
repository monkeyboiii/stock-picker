import os
from datetime import date, datetime

import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
from pandas import DataFrame
from loguru import logger

from app.constant.confirm import confirms_execution
from app.db.engine import engine_from_env
from app.filter.tail_scraper import filter_desired
from app.profile.tracer import trace_elapsed


load_dotenv(override=True)


scopes = [
    "https://www.googleapis.com/auth/spreadsheets"
]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)
sheet_id = os.getenv("GOOGLE_SHEET_ID")
if sheet_id is None:
    raise ValueError("Missing or incorrect GOOGLE_SHEET_ID")


def client_from_env():
    return client


def get_stock_sheet():
    return client.open_by_key(sheet_id)
    

@trace_elapsed(unit='s')
def add_df_to_new_sheet(trade_day: date, df: DataFrame) -> None:
    sheet = get_stock_sheet()
    title = trade_day.isoformat()

    if df.shape[0] == 0:
        logger.warning(f"No data to update for {title}")
        return

    rows = max((df.shape[0] + 1) * 2, 50)
    columns = df.shape[1] + 10
    
    logger.info(f"Creating/updating sheet {title} of {rows} rows and {columns} columns")
    try:
        worksheet = sheet.worksheet(title)
        logger.warning(f"Sheet {title} already exists, overwriting it")
        confirms_execution()
        worksheet.clear()

    except WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=title, rows=rows, cols=columns)

    df = df.map(str)
    worksheet.update([df.columns.values.tolist()] + df.values.tolist()) # type: ignore


if __name__ == "__main__":
    from app.constant.schedule import previous_trade_day
    
    trade_day = previous_trade_day(date(2025, 2, 9))
    df = filter_desired(engine_from_env(), trade_day, dryrun=True)
    add_df_to_new_sheet(trade_day, df)
