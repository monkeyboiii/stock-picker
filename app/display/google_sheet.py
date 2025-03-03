import os
from datetime import date
from typing import Optional

import gspread
from gspread.exceptions import WorksheetNotFound
from gspread_formatting import CellFormat, TextFormat, format_cell_ranges # type: ignore
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
from pandas import DataFrame
from loguru import logger

from app.constant.confirm import confirms_execution
from app.db.engine import engine_from_env
from app.db.models import FeedDaily
from app.display.utils import get_color_for_column
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
def add_df_to_new_sheet(trade_day: date, df: DataFrame, yes: Optional[bool] = False) -> None:
    title = trade_day.isoformat()
    if df.shape[0] == 0:
        logger.warning(f"No data to update for {title}")
        return
    assert len(FeedDaily.feed_column_mapping().keys()) <= 26


    def idx_to_alpha(idx: int):
        return chr(idx + 65)
    
    feed_column_map = FeedDaily.feed_column_mapping()
    color_df = df[FeedDaily.colorize_columns()].apply(get_color_for_column)
    color_formats = [
        (
            c, idx, CellFormat(backgroundColor=color)
        ) for c in color_df.columns for idx, color in enumerate(color_df[c]) if color.blue != 1.0
    ]
    feed_df = FeedDaily.convert_to_feed(df)
    alpha_map = {c: idx_to_alpha(idx) for idx, c in enumerate(feed_column_map.keys())}

    #
    # make sheet
    rows = max((feed_df.shape[0] + 1) * 2, 50)
    columns = feed_df.shape[1] + 10
    
    logger.info(f"Creating/updating sheet {title} of {rows} rows and {columns} columns")
    try:
        sheet = get_stock_sheet()
        worksheet = sheet.worksheet(title)
        confirms_execution(f"Overwriting exsting Sheet {title}", yes=yes)
        worksheet.clear()

    except WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=title, rows=rows, cols=columns)

    #
    # fill sheet
    feed_df = feed_df.map(str)
    worksheet.update([feed_df.columns.values.tolist()] + feed_df.values.tolist()) # type: ignore

    #
    # format sheet
    format_cell_ranges(worksheet, [
        (
            f"{alpha_map[c]}{row_number+2}",
            format_
        ) for c, row_number, format_ in color_formats
    ] + [
        (
            alpha_map[c], 
            CellFormat(
                horizontalAlignment='RIGHT',
                textFormat=TextFormat(
                    bold=True
                )
            )
        ) for c in FeedDaily.right_align_columns()
    ])    
    logger.success(f"Google sheet updated for {title}")


if __name__ == "__main__":
    from app.constant.schedule import previous_trade_day
    
    trade_day = previous_trade_day(date(2025, 2, 24))
    df = filter_desired(engine_from_env(), trade_day, dryrun=True)
    add_df_to_new_sheet(
        trade_day=trade_day,
        df=df,
        yes=True,
    )
