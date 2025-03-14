from decimal import Decimal
from typing import Mapping
from pydantic import BaseModel

from backtest.const.valuable import Currency
from backtest.model.position.base import PositionBase


class BaseQuote(BaseModel):
    base: Currency
    quote: Currency
    ratio: Decimal


class CashPosition(PositionBase):
    currencies: Mapping[Currency, Decimal]

    def __init__(self, currencies: Mapping[Currency, Decimal] = {}):
        self.currencies = currencies

    async def valuation_in(self, currency: Currency) -> Decimal:
        # TODO SPFA: https://chatgpt.com/share/67d2ec46-5e24-800f-bd98-01edce509bbf
        pass

    async def exchange(self, base: Currency, quote: Currency, amount: Decimal) -> BaseQuote:
        pass
