from decimal import Decimal

from app.constant.exchange import (
    SEX_SHANGHAI,
    SEX_SHENZHEN,
    SEX_BEIJING,
)
from backtest.model.fee import StockFee


_FLAT__5 = Decimal("5")

_BPS__5 = Decimal("0.000_5")
_BPS__4 = Decimal("0.000_4")
_BPS__3 = Decimal("0.000_3")
_BPS__2d5 = Decimal("0.000_25")
_BPS__2 = Decimal("0.000_2")
_BPS__1 = Decimal("0.000_1")

_BPS__0d2 = Decimal("0.000_02")

_BPS__NAUGHT = Decimal()


# commission general map
FEE_GMAP = {
    SEX_SHANGHAI: StockFee(
        stamp           =_BPS__5,
        transfer        =_BPS__2,
        commission      =_BPS__3,
        min_commission  =_FLAT__5,
    ),
    SEX_SHENZHEN: StockFee(
        stamp           =_BPS__5,
        transfer        =_BPS__NAUGHT,
        commission      =_BPS__3,
        min_commission  =_FLAT__5,
    ),
    SEX_BEIJING: StockFee(
        stamp           =_BPS__5,
        transfer        =_BPS__NAUGHT,
        commission      =_BPS__3,
        min_commission  =_FLAT__5,
    ),
}


if __name__ == "__main__":
    from loguru import logger
    
    egf = StockFee(
        stamp           =_BPS__5,
        transfer        =_BPS__0d2,
        commission      =_BPS__2d5,
        min_commission  =_FLAT__5,
    )
    buy_args = {
        "price": Decimal("10"),
        "amount": 10000,
        "direction": "buy",
    }
    sell_args = {
        "price": Decimal("10"),
        "amount": 10000,
        "direction": "sell",
    }
    
    assert egf.calculate_cost(**buy_args) == Decimal("10_0027") # type: ignore
    assert egf.calculate_cost(**sell_args) == Decimal("9_9923") # type: ignore

    logger.info(f"shanghai adjusted price {egf.adjust_price(**buy_args)}") # type: ignore
    logger.info(f"shanghai adjusted price {egf.adjust_price(**sell_args)}") # type: ignore
    