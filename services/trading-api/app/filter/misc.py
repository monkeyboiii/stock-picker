from enum import Enum


class StockFilter(Enum):
    TAIL_SCRAPER = 1


# Create bidirectional map
filter_to_id = {f.name: f.value for f in StockFilter}
id_to_filter = {f.value: f.name for f in StockFilter}


def get_filter_id(sf: StockFilter) -> int:
    return filter_to_id[sf.name]


def get_filter_name(sf: StockFilter) -> str:
    return "-".join(id_to_filter[sf.value].lower().split("_"))


def get_filter_canonical_name(sf: StockFilter) -> str:
    return id_to_filter[sf.value]


if __name__ == '__main__':
    assert get_filter_id(StockFilter.TAIL_SCRAPER) == 1
    assert get_filter_name(StockFilter.TAIL_SCRAPER) == "tail-scraper"
    assert get_filter_canonical_name(StockFilter.TAIL_SCRAPER) == "TAIL_SCRAPER"
