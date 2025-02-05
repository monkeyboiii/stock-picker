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
    
    df_daily = pull_stock_daily()
    print(df_daily)

    # df_daily_hist = pull_stock_daily_hist(
    #     '870508', 
    #     date(2024, 1, 1), 
    #     date(2024, 1, 31), 
    #     'qfq'
    # )
    # print(df_daily_hist)
