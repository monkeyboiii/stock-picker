from datetime import date

from pandas import DataFrame
import akshare as ak

from app.constant.exchange import *


market_map = {
    SEX_SHANGHAI: ak.stock_sh_a_spot_em,
    SEX_SHENZHEN: ak.stock_sz_a_spot_em,
    SEX_BEIJING: ak.stock_bj_a_spot_em,
    SEX_HONGKONG: ak.stock_hk_spot_em,
}


def pull_stock(exchnage: str) -> DataFrame:
    if exchnage not in market_map.keys():
        raise ValueError(f"exchange {exchnage} not supported")

    return market_map[exchnage]()


def pull_stock_daily() -> DataFrame:
    return ak.stock_zh_a_spot_em()


def pull_stock_daily_hist(symbol: str, start_date: date, end_date: date, adjust: str) -> DataFrame:
    return ak.stock_zh_a_hist(
        symbol=symbol, 
        period="daily", 
        start_date=start_date.strftime('%Y%m%d'),
        end_date=end_date.strftime('%Y%m%d'), 
        adjust=adjust or 'qfq'
    )


if __name__ == '__main__':
    # df = pull_stock(SEX_SHANGHAI)
    # print(df)
    
    from decimal import Decimal
    from pandas import notna
    df_daily = pull_stock_daily()
    df_column_mapping = {
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
        'open': lambda x: Decimal(format(x, '.3f')) if notna(x) else x,
        'high': lambda x: Decimal(format(x, '.3f')) if notna(x) else x,
        'low': lambda x: Decimal(format(x, '.3f')) if notna(x) else x,
        'close': lambda x: Decimal(format(x, '.3f')) if notna(x) else x,
        'volume': lambda x: int(x) if notna(x) else x,
        'turnover': lambda x: int(x) if notna(x) else x,
        'capital': lambda x: int(x) if notna(x) else x,
        'circulation_capital': lambda x: int(x) if notna(x) else x,
        'quantity_relative_ratio': lambda x: float(x) if notna(x) else x,
        'turnover_rate': lambda x: float(x) if notna(x) else x,
    }
    df = df_daily.rename(columns=df_column_mapping)[list(df_column_mapping.values())]
    for col, func in transformations.items():
        df[col] = df[col].apply(func)
    print(df)

    # df_daily_hist = pull_stock_daily_hist(
    #     '870508', 
    #     date(2024, 1, 1), 
    #     date(2024, 1, 31), 
    #     'qfq'
    # )
    # print(df_daily_hist)
