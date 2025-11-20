from datetime import date
from decimal import Decimal

import akshare as ak  # type: ignore
from loguru import logger
from pandas import DataFrame, notna
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.constant.collection import CollectionType
from app.constant.exchange import (
    SEX_BEIJING,
    SEX_CHINA_MAINLAND,
    SEX_HONGKONG,
    SEX_SHANGHAI,
    SEX_SHENZHEN,
)

# Retry configuration for external API calls
# Retry on common network errors with exponential backoff
# Max 4 attempts: 2s, 4s, 8s (total ~14s max wait)
RETRY_CONFIG = {
    "retry": retry_if_exception_type((ConnectionError, TimeoutError, Exception)),
    "stop": stop_after_attempt(4),
    "wait": wait_exponential(multiplier=1, min=2, max=10),
    "reraise": True,
}


market_map = {
    SEX_CHINA_MAINLAND: ak.stock_zh_a_spot_em,
    SEX_SHANGHAI:       ak.stock_sh_a_spot_em,
    SEX_SHENZHEN:       ak.stock_sz_a_spot_em,
    SEX_BEIJING:        ak.stock_bj_a_spot_em,
    SEX_HONGKONG:       ak.stock_hk_spot_em,
}


@retry(**RETRY_CONFIG)
def pull_stocks(exchange: str) -> DataFrame:
    '''
    Pulls basic stocks info for a given market.

    RESILIENCE: Retries up to 4 times with exponential backoff on API failures.
    '''

    if exchange not in market_map.keys():
        raise ValueError(f"exchange {exchange} not supported")

    logger.debug(f"Fetching stocks for exchange: {exchange}")

    column_mapping = {
        '代码': 'code',
        '名称': 'name',
    }

    try:
        df = market_map[exchange]()
    except Exception as e:
        logger.error(f"Error fetching stocks from AKShare for {exchange}: {e}")
        raise

    df = df.rename(columns=column_mapping)[list(column_mapping.values())]
    logger.debug(f"Fetched {len(df)} stocks for exchange: {exchange}")

    return df


@retry(**RETRY_CONFIG)
def pull_collections(cType: CollectionType) -> DataFrame:
    '''
    RESILIENCE: Retries up to 4 times with exponential backoff on API failures.
    '''
    logger.debug(f"Fetching collections for type: {cType}")

    column_mapping = {
        '板块名称': 'name',
        '板块代码': 'code',
    }

    try:
        match cType:
            case CollectionType.INDUSTRY_BOARD:
                df = ak.stock_board_industry_name_em()

            case _:
                raise Exception("Not implemented yet!")
    except Exception as e:
        logger.error(f"Error fetching collections from AKShare for {cType}: {e}")
        raise

    logger.debug(f"Fetched {len(df)} collections for type: {cType}")
    return df.rename(columns=column_mapping)[list(column_mapping.values())]


@retry(**RETRY_CONFIG)
def pull_stocks_in_collection(cType: CollectionType, symbol: str) -> DataFrame:
    '''
    RESILIENCE: Retries up to 4 times with exponential backoff on API failures.
    '''
    logger.debug(f"Fetching stocks in collection: {symbol} (type: {cType})")

    column_mapping = {
        '代码': 'code',
        '名称': 'name',
    }

    try:
        match cType:
            case CollectionType.INDUSTRY_BOARD:
                df = ak.stock_board_industry_cons_em(symbol=symbol)

            case _:
                raise Exception("Not implemented yet!")
    except Exception as e:
        logger.error(f"Error fetching stocks in collection {symbol} from AKShare: {e}")
        raise

    logger.debug(f"Fetched {len(df)} stocks in collection: {symbol}")
    return df.rename(columns=column_mapping)[list(column_mapping.values())]


@retry(**RETRY_CONFIG)
def pull_stock_daily() -> DataFrame:
    '''
    Ensures stocks are eligible for insertion.

    Ineligible:
        1. stock with no close or trade volume

    RESILIENCE: Retries up to 4 times with exponential backoff on API failures.
    '''
    logger.debug("Fetching daily stock data from AKShare")

    column_mapping = {
        '代码': 'code',
        '今开': 'open',
        '最高': 'high',
        '最低': 'low',
        '最新价': 'close',
        '成交量': 'volume',
        '成交额': 'turnover',
        '总市值': 'capital',
        '流通市值': 'circulation_capital',
        '量比': 'quantity_relative_ratio',
        '换手率': 'turnover_rate',
    }
    transformations = {
        'open':                     lambda x: Decimal(format(x, '.3f')) if notna(x) else x,
        'high':                     lambda x: Decimal(format(x, '.3f')) if notna(x) else x,
        'low':                      lambda x: Decimal(format(x, '.3f')) if notna(x) else x,
        'close':                    lambda x: Decimal(format(x, '.3f')) if notna(x) else x,
        'volume':                   lambda x: round(x) if notna(x) else x,
        'turnover':                 lambda x: round(x) if notna(x) else x,
        'capital':                  lambda x: round(x) if notna(x) else x,
        'circulation_capital':      lambda x: round(x) if notna(x) else x,
        'quantity_relative_ratio':  lambda x: round(x, 3) if notna(x) else x,
        'turnover_rate':            lambda x: round(x, 3) if notna(x) else x,
    }

    try:
        df = ak.stock_zh_a_spot_em()
    except Exception as e:
        logger.error(f"Error fetching daily stock data from AKShare: {e}")
        raise

    df = df.rename(columns=column_mapping)[list(column_mapping.values())]
    df = df[df['close'].notna() & df['volume'].notna()]
    for col, func in transformations.items():
        df[col] = df[col].apply(func)

    logger.debug(f"Fetched {len(df)} stocks daily data")
    return df


@retry(**RETRY_CONFIG)
def pull_stock_daily_hist(symbol: str, start_date: date, end_date: date, adjust: str = 'qfq') -> DataFrame:
    '''
    Ensures stocks are eligible for insertion.

    Ineligible:
        1. stock with no close or trade volume

    RESILIENCE: Retries up to 4 times with exponential backoff on API failures.
    '''
    logger.debug(f"Fetching historical data for {symbol} from {start_date} to {end_date}")

    column_mapping = {
        '日期': 'trade_day',
        '开盘': 'open',
        '最高': 'high',
        '最低': 'low',
        '收盘': 'close',
        '成交量': 'volume',
        '成交额': 'turnover',
    }
    transformations = {
        'open':                     lambda x: Decimal(format(x, '.3f')) if notna(x) else x,
        'high':                     lambda x: Decimal(format(x, '.3f')) if notna(x) else x,
        'low':                      lambda x: Decimal(format(x, '.3f')) if notna(x) else x,
        'close':                    lambda x: Decimal(format(x, '.3f')) if notna(x) else x,
        'volume':                   lambda x: round(x) if notna(x) else x,
        'turnover':                 lambda x: round(x) if notna(x) else x,
    }

    try:
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date.strftime('%Y%m%d'),
            end_date=end_date.strftime('%Y%m%d'),
            adjust=adjust,
        )
    except Exception as e:
        logger.error(f"Error fetching historical data for {symbol} from AKShare: {e}")
        raise

    if len(df) == 0:
        logger.debug(f"No data returned for {symbol}")
        return DataFrame(index=range(0), columns=list(column_mapping.values()))

    df = df.rename(columns=column_mapping)[list(column_mapping.values())]
    df = df[df['close'].notna() & df['volume'].notna()]
    for col, func in transformations.items():
        df[col] = df[col].apply(func)

    logger.debug(f"Fetched {len(df)} records for {symbol}")
    return df


@retry(**RETRY_CONFIG)
def pull_collection_daily(cType: CollectionType) -> DataFrame:
    '''
    RESILIENCE: Retries up to 4 times with exponential backoff on API failures.
    '''
    logger.debug(f"Fetching collection daily data for type: {cType}")

    column_mapping = {
        # '排名': 'rank', # TODO maybe useful in deciding hot
        '板块代码': 'code',
        '最新价': 'price',
        '涨跌额': 'change',
        '涨跌幅': 'change_rate',
        '总市值': 'capital',
        '换手率': 'turnover_rate',
        '上涨家数': 'gainer_count',
        '下跌家数': 'loser_count',
        '领涨股票': 'top_gainer',
        '领涨股票-涨跌幅': 'top_gain',
    }
    transformations = {
        'price':                    lambda x: Decimal(format(x, '.3f')) if notna(x) else x,
        'change':                   lambda x: Decimal(format(x, '.3f')) if notna(x) else x,
        'change_rate':              lambda x: round(x, 3) if notna(x) else x,
        'capital':                  lambda x: round(x) if notna(x) else x,
        'turnover_rate':            lambda x: round(x, 3) if notna(x) else x,
        'top_gain':                 lambda x: round(x, 3) if notna(x) else x,
    }

    try:
        match cType:
            case CollectionType.INDUSTRY_BOARD:
                df = ak.stock_board_industry_name_em()

            case _:
                raise Exception("Not implemented yet!")
    except Exception as e:
        logger.error(f"Error fetching collection daily data from AKShare for {cType}: {e}")
        raise

    df = df.rename(columns=column_mapping)[list(column_mapping.values())]
    df = df.dropna()
    for col, func in transformations.items():
        df[col] = df[col].apply(func)

    logger.debug(f"Fetched {len(df)} collection daily records for type: {cType}")
    return df


if __name__ == '__main__':
    # df = pull_stocks(SEX_SHANGHAI)
    # print(df.head(10))

    df = pull_stocks(SEX_CHINA_MAINLAND)
    print(df.shape)
    print(df.head(10))

    # df_collections = pull_collections(CollectionType.INDUSTRY_BOARD)
    # print(df_collections.head(10))

    # df_sic = pull_stocks_in_collection(CollectionType.INDUSTRY_BOARD, '小金属')
    # print(df_sic.head(10))

    # df_daily = pull_stock_daily()
    # print(df_daily.head(10))

    # df_daily_hist = pull_stock_daily_hist(
    #     '870508',
    #     date(2024, 1, 1),
    #     date(2024, 1, 31),
    #     'qfq'
    # )
    # print(df_daily_hist.head(10))

    # df = pull_collection_daily(CollectionType.INDUSTRY_BOARD)
    # print(df.head(10))
