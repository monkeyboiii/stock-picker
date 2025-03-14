# Stock Picker

This application is designed to set up and run a stock picker that
1. refreshes stock data;
2. calculates derived metrics, such as moving averages and ranking;
3. filters desired stocks based on certain criteria;
4. displays filtered stocks into configured output.

These tasks are run based on .env file and command line options.

## State of Database

It's recommended to use ***PostgreSQL*** version 16 or higher. The database can be in one of the several states below.

1. Not or partially initialized.
2. Intialized with basic market info in the `market`, `stock`, `collection` table.
3. Partially filled `stock_daily` and `collection_daily` tables.
4. Up-to-date (trade_day) `stock_daily` and `collection_daily` tables, but derived metrics are not or partially calculated.
5. Up-to-date (trade_day) `stock_daily` and `collection_daily` tables, and up-to-date derived metrics.

In state 5, the database is ready to run daily stock picking automation.

## Usage

Since the application is designed to be run from different states of the database, it
needs to be run in a specific order using different subcommands to ensure that the data is
up-to-date and correct.

### Preparation

Dependencies

```sh
pip install -r requirements.txt
```

Environment

* ***PostgreSQL*** database
* `.env` file
* google `credentials.json` for result upload

### Execution

```sh
# can use module level import
export PYTHONPATH=.

python app/main.py [-h|--help]
```

#### Init
This corresponds to state 1 -> state 2 transition.
```sh
python app/main.py init -r -lll
```

#### Run

This command can be run at state 2/3/4, which will push the state to 5.
And once at state 5, you can schedule to run this command on a daily basis.
```sh
python app/main.py run
```

If you do not want to filter just yet, this command corresponds to state 2 -> state 3/4 transition.
Since natural days go by and trade data may become outdated on a daily basis, this corresponds to the everyday state update from 5 -> 3 -> 4.
```sh
python app/main.py run -t ingest
```

This command changes state form 4 -> 5
```sh
python app/main.py run -t update
```

This command filters and displays into configurec output. 
```sh
python app/main.py run -t display
```

#### reset

This corresponds to state 2/3/4/5 -> state 1/2 transition.
```sh
python app/main.py reset
```


## TODOs

- [x] redesign FeedDaily
- [x] google sheet update
- [ ] backtests
- [ ] async engine
- [ ] get state of database, visual inspection for observability
- [ ] later insert of ma250 from materialized view, or drop ma250 from db
- [ ] real time data from 2:30 to 3:00 (akshare/openD)
- [ ] real time data from 2:30 to 3:00 (akshare/openD)
- [ ] reset db