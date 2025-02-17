from datetime import date
from decimal import Decimal

import akshare as ak # type: ignore
from pandas import DataFrame, notna

from app.constant.collection import *
from app.constant.exchange import *


market_map = {
    SEX_CHINA_MAINLAND: ak.stock_zh_a_spot_em,
    SEX_SHANGHAI:       ak.stock_sh_a_spot_em,
    SEX_SHENZHEN:       ak.stock_sz_a_spot_em,
    SEX_BEIJING:        ak.stock_bj_a_spot_em,
    SEX_HONGKONG:       ak.stock_hk_spot_em,
}


def pull_stocks(exchange: str) -> DataFrame:
    '''
    Pulls basic stocks info for a given market.
    '''

    if exchange not in market_map.keys():
        raise ValueError(f"exchange {exchange} not supported")

    column_mapping = {
        '代码': 'code',
        '名称': 'name',
    }
    df = market_map[exchange]()
    df = df.rename(columns=column_mapping)[list(column_mapping.values())]

    return df


def pull_collections(cType: CollectionType) -> DataFrame:
    column_mapping = {
        '板块名称': 'name',
        '板块代码': 'code',
    }

    match cType:
        case CollectionType.INDUSTRY_BOARD:
            df = ak.stock_board_industry_name_em()

        case _:
            raise Exception("Not implemented yet!")

    return df.rename(columns=column_mapping)[list(column_mapping.values())]


def pull_stocks_in_collection(cType: CollectionType, symbol: str) -> DataFrame:
    column_mapping = {
        '代码': 'code',
        '名称': 'name',
    }

    match cType:
        case CollectionType.INDUSTRY_BOARD:
            df = ak.stock_board_industry_cons_em(symbol=symbol)

        case _:
            raise Exception("Not implemented yet!")

    return df.rename(columns=column_mapping)[list(column_mapping.values())]


def pull_stock_daily() -> DataFrame:
    '''
    Ensures stocks are eligible for insertion.

    Ineligible:
        1. stock with no close or trade volume
    '''

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

    df = ak.stock_zh_a_spot_em()
    df = df.rename(columns=column_mapping)[list(column_mapping.values())]
    df = df[df['close'].notna() & df['volume'].notna()]
    for col, func in transformations.items():
        df[col] = df[col].apply(func)

    return df


def pull_stock_daily_hist(symbol: str, start_date: date, end_date: date, adjust: str = 'qfq') -> DataFrame:
    '''
    Ensures stocks are eligible for insertion.

    Ineligible:
        1. stock with no close or trade volume
    '''

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

    df = ak.stock_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=start_date.strftime('%Y%m%d'),
        end_date=end_date.strftime('%Y%m%d'),
        adjust=adjust,
    )

    if len(df) == 0:
        return DataFrame(index=range(0), columns=list(column_mapping.values()))

    df = df.rename(columns=column_mapping)[list(column_mapping.values())]
    df = df[df['close'].notna() & df['volume'].notna()]
    for col, func in transformations.items():
        df[col] = df[col].apply(func)

    return df


def pull_collection_daily(cType: CollectionType) -> DataFrame:
    column_mapping = {
        '排名': 'rank',
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

    match cType:
        case CollectionType.INDUSTRY_BOARD:
            df = ak.stock_board_industry_name_em()

        case _:
            raise Exception("Not implemented yet!")

    df = df.rename(columns=column_mapping)[list(column_mapping.values())]
    df = df.dropna()
    for col, func in transformations.items():
        df[col] = df[col].apply(func)

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