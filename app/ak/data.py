from datetime import date

from pandas import DataFrame, notna
from decimal import Decimal
import akshare as ak

from app.constant.exchange import *


market_map = {
    SEX_SHANGHAI: ak.stock_sh_a_spot_em,
    SEX_SHENZHEN: ak.stock_sz_a_spot_em,
    SEX_BEIJING:  ak.stock_bj_a_spot_em,
    SEX_HONGKONG: ak.stock_hk_spot_em,
}


def pull_stock(exchnage: str) -> DataFrame:
    '''
    Pulls basic stocks info for a given market.
    '''

    if exchnage not in market_map.keys():
        raise ValueError(f"exchange {exchnage} not supported")

    return market_map[exchnage]()


def pull_stock_daily(today: date) -> DataFrame:
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
    df['trade_day'] = today

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


if __name__ == '__main__':
    # df = pull_stock(SEX_SHANGHAI)
    # print(df.head(10))
    
    # df_daily = pull_stock_daily(date.today())
    # print(df_daily.head(10))

    df_daily_hist = pull_stock_daily_hist(
        '870508', 
        date(2024, 1, 1), 
        date(2024, 1, 31), 
        'qfq'
    )
    print(df_daily_hist.head(10))

