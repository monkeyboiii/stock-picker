from abc import ABC, abstractmethod
from decimal import Decimal
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


class FeeType(Enum):
    STOCK = "stock"
    CRYPTO = "fixed"
    NONE = "none"


class FeeBase(BaseModel, ABC):
    type: FeeType = Field(...)

    @abstractmethod
    def calculate_cost(
        self, price: Decimal, amount: int, direction: Literal["buy", "sell"]
    ) -> Decimal:
        raise NotImplementedError

    @abstractmethod
    def adjust_price(
        self, pprice: Decimal, amount: int, direction: Literal["buy", "sell"]
    ) -> Decimal:
        raise NotImplementedError


class StockFee(FeeBase):
    type: Literal[FeeType.STOCK] = Field(default=FeeType.STOCK)

    # rate
    stamp: Decimal
    transfer: Decimal
    commission: Decimal

    # minimum charge
    min_commission: Decimal = Decimal()

    def calculate_cost(
        self,
        price: Decimal,
        amount: int,
        direction: Literal["buy", "sell"] = "buy",
    ) -> Decimal:
        commission = max(self.min_commission, price * amount * self.commission)
        match direction:
            case "buy":
                return price * amount * (1 + self.transfer) + commission
            case "sell":
                return price * amount * (1 - self.stamp - self.transfer) - commission
            case _:
                assert False, f"Invalid direction: {direction}"

    def adjust_price(
        self,
        price: Decimal,
        amount: int,
        direction: Literal["buy"] | Literal["sell"] = "buy",
    ) -> Decimal:
        if amount == 0:
            return price
        return (
            self.calculate_cost(price=price, amount=amount, direction=direction)
            / amount
        )


class CryptoFee(FeeBase):
    type: Literal[FeeType.CRYPTO] = Field(default=FeeType.CRYPTO)

    def calculate_cost(
        self, price: Decimal, amount: int, direction: Literal["buy", "sell"] = "buy"
    ) -> Decimal:
        raise NotImplementedError

    def adjust_price(
        self, price: Decimal, amount: int, direction: Literal["buy", "sell"] = "buy"
    ) -> Decimal:
        raise NotImplementedError
