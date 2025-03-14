from pydantic import BaseModel
from datetime import datetime
from typing import Tuple

from backtest.position import Position


class Estimator(BaseModel):
    duration: Tuple[datetime, datetime]
    position: Position

    async def simulate(self):
        pass