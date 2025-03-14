from backtest.model.position.cash import CashPosition
from backtest.model.position.spot import SpotPosition


class UnifiedPosition:
    cash: CashPosition | None
    spot: SpotPosition | None
    # margin
    # future
    # option

    