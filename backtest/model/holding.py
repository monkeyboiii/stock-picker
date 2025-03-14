from typing import Optional
from pydantic import BaseModel
from decimal import Decimal
from datetime import datetime

from backtest.const.valuable import Currency
from backtest.model.fee import StockFee


class Stock(BaseModel):
    code: str
    name: str
    market: str
    quote: Currency
    
    holding: int = 0 # one share
    carrying_cost: Decimal = Decimal()
    fee: StockFee

    @property
    def carrying_cost_per_share(self) -> Decimal:
        return self.carrying_cost / self.holding if self.holding > 0 else Decimal()

    def buy(self, amount: int, price: Decimal):
        self.holding += amount
        self.carrying_cost += self.commission.calculate_cost(
            price=price, 
            amount=amount, 
            direction="buy"
        )
    
    def sell(self, amount: int, price: Decimal):
        self.holding -= amount
        self.carrying_cost -= self.commission.calculate_cost(
            price=price, 
            amount=amount, 
            direction="sell"
        )

    async def get_close_at(self, trade_time: datetime) -> Decimal:
        assert trade_time is not None

        # TODO: get close price from database
        return Decimal("1.234")

    async def market_value(self, trade_time: Optional[datetime] = datetime.now()) -> Decimal:
        if not trade_time:
            trade_time = datetime.now()
            # TODO: trade_time = previous_trade_time(self.market, datetime.now())
        
        close = await self.get_close_at(trade_time)
        return close * self.holding


# class Crypto(BaseModel):
#     code: str
#     name: str
#     exchange: str
#     precision: int # decimal places of the smallest unit
    
#     holding: int = 0 # one smallest unit
#     carrying_cost: Decimal = Decimal()

#     def quote(self, curreny: Optional[Currency] = None, isSelf: Optional[bool] = False):
#         pass

#     def purchase(self, amount: int | Decimal, price: Decimal, isUnit: Optional[bool] = False):
#         if not isUnit:
#             amount = amount * 18 ** self.precision
#         self.holding += amount
#         self.carrying_cost += amount * price

#     async def get_close_at(self, trade_time: datetime) -> Decimal:
#         assert trade_time is not None

#         return Decimal("1.234")

#     @computed_field
#     @property
#     async def market_value(self, trade_time: Optional[datetime] = datetime.now()):
#         if not trade_time:
#             trade_time = datetime.now()
#             # TODO trade_time = previous_trade_time(self.market, datetime.now())
        
#         close = await self.get_close_at(trade_time)
#         return close * self.holding


if __name__ == '__main__':
    import asyncio
    
    async def main(stock: Stock):
        return await stock.market_value()

    stock = Stock(
        code="AAPL", 
        name="Apple Inc.", 
        market="NASDAQ", 
        quote=Currency.USD,
        fee=
    )

    stock.buy(100, Decimal("123.45"))

    print(asyncio.run(main(stock)))