from enum import Enum


class CollectionType(Enum):
    ANALYST = 'a'
    BOARD = 'b'
    CONCEPT_BOARD = 'c'
    INDUSTRY_BOARD = 'i'
    INDEX = 'x'


# b
# 主板
COLLECTION_BOARD_MAIN = 'main'
# 创业板
COLLECTION_BOARD_ChiNext = 'ChiNext'


# i
COLLECTION_BOARD_INDUSTRY = 'industry'


# c
COLLECTION_BOARD_CONCEPT = 'concept'