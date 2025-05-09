# STOCK EXCHANGE
SEX_CHINA_MAINLAND = 'CHN'
SEX_SHANGHAI = 'SSE'
SEX_SHENZHEN = 'SZSE'
SEX_BEIJING = 'BSE'
SEX_HONGKONG = 'HKEX'


# CRYPTO EXCHANGE
CEX_BINANCE = 'BINANCE'
CEX_OKC = 'OKX'


MARKET_SUPPORTED = set([SEX_SHANGHAI, SEX_SHENZHEN, SEX_BEIJING])
MARKET_UNSUPPORTED = set([SEX_HONGKONG])


BAD_STOCKS = set([
    '600631', # 百联股份 退市
    '600832', # 东方明珠 退市
    '833994', # 翰博高斯
    '833874', # 泰祥股份
    '831834',
])