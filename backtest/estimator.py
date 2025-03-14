from enum import Enum
from pydantic import BaseModel
from datetime import datetime
from typing import Tuple


class KindEnum(Enum):
    SINGLE_ASSET: int = 0
    MULTI_ASSET:  int = 1


class Estimator(BaseModel):
    duration: Tuple[datetime, datetime]
    position: UnifiedPosition
    kind: KindEnum

    async def simulate(self):
        pass