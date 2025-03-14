from enum import Enum


class CommonAssets(Enum):
    BTC = "BTC"


class Currency(Enum):
    CNY = "CNY"
    USD = "USD"
    USDT = "USDT"
    BTC = CommonAssets.BTC.value


class Asset(Enum):
    BTC = CommonAssets.BTC.value


class AssetType(Enum):
    CURRENY = "currency"
    STOCK = "stock"
    FUND = "fund"
    BOND = "bond"

    CRYPTO = "crypto"

    FUTURE = "future"
    OPTION = "option"