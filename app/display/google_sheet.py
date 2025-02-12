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
from app.utils.filter import filter_desired


load_dotenv()


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
    

def add_df_to_new_sheet(trade_day: date, df: DataFrame) -> None:
    title = trade_day.isoformat()
    if df.shape[0] == 0:
        logger.warning(f"No data to update for {title}")
        return

    start = datetime.now()
    sheet = get_stock_sheet()

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

    elapsed_ms = round((datetime.now() - start).total_seconds() * 1000)
    logger.info(f"Sheet {title} updated in {elapsed_ms} ms")


def live_update(trade_day: date, df_new: DataFrame) -> None:
    start = datetime.now()
    sheet = get_stock_sheet()
    title = trade_day.isoformat()
    columns = df_new.columns.to_list()
    columns_to_compare = list(set(columns).difference(['code', 'trade_day', 'name']))

    try:
        worksheet = sheet.worksheet(title)
        data = worksheet.get_all_values()
        popped_columns = data.pop(0)
        assert columns == popped_columns
        # possible to do filtering or concatenating
        df_old = DataFrame(columns=columns, data=data)
        
        df_old.set_index('code', inplace=True)
        df_new.set_index('code', inplace=True)

        all_codes = df_old.index.union(df_new.index)

        df_old = df_old.reindex(all_codes)
        df_new = df_new.reindex(all_codes)

        # Compare only the specified columns
        df_diff = df_old[columns_to_compare] != df_new[columns_to_compare]

        df_changes = df_old.copy()

        for col in columns_to_compare:
            df_changes[col] = df_new[col].where(df_diff[col], None)

        # Reset index and keep only rows with changes
        df_changes.reset_index(inplace=True)
        df_changes.dropna(how="all", subset=columns_to_compare, inplace=True) 
        

    except WorksheetNotFound:
        logger.error(f'Worksheet for stock {title} not found')
        return

    elapsed_ms = round((datetime.now() - start).total_seconds() * 1000)
    logger.info(f"Sheet {title} updated in {elapsed_ms} ms")



if __name__ == "__main__":
    # from app.constant.schedule import previous_trade_day
        
    # trade_day = previous_trade_day(date(2025, 2, 11))
    # df = filter_desired(engine_from_env(), trade_day, dryrun=True)
    # add_df_to_new_sheet(trade_day, df)

    from datetime import timedelta

    df_new = filter_desired(engine_from_env(), date(2025, 2, 11), dryrun=True)
    df_new['trade_day'] = df_new['trade_day'].apply(lambda d: d - timedelta(days=1))
    live_update(date(2025, 2, 10), df_new)
