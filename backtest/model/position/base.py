from abc import ABC, abstractmethod

from backtest.const.valuable import Currency


class PositionBase(ABC):
    @abstractmethod
    async def valuation_in(self, currency: Currency):
        pass
