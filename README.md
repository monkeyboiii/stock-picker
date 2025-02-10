# Stock Picker

This application is designed to set up and run a stock picker that
1. refreshes stock data,
2. calculates derived metrics, such as moving averages,
3. filters desired stocks based on certain criteria,
4. displays filtered stocks into configured output.

These tasks are run based on .env file and command line options.

## Usage

Since the applciation is designed to be run from different states of the database, it
needs to be run in a specific order using different subcommands to ensure that the data is
up-to-date and correct.

### Install Dependencies

```sh
pip install -r requirements
```

### State of Database

It's recommended to use `PostgreSQL` version 16 or higher. The database can be in one of
the several states below.

1. Not initialized.
2. Intialized with basic market info in the `market` table, and rest tables are empty.
3. No or partially filled `stock` and `stock_daily` tables.
4. `stock` and `stock_daily` and filled with up-to-date (trade_day) data, but derived metrics are not or partially calculated.
5. `stock` and `stock_daily` and filled with up-to-date (trade_day) data, and derived metrics are calculated.

In state 5, the database is ready to run daily stock picking automation.

### List of Common Command

When running in script mode

```sh
PYTHONPATH=. python app/main.py <subcommand> [-option=[value]]*
```

When running the packed executable

```sh
# build from src app/
pyinstaller –onefile –windowed myscript.py

# run
./main <subcommand> [-option=[value]]*
```

The rest of the document will use `cmd` as shorthand for either of them.


#### Init
This corresponds to state 1 -> state 2 transition.
```sh
init ...
```

#### Fill
This corresponds to state 2 -> state 3/4 transition.
```sh
fill ...
```

#### Run
Since natural days go by and trade data may become outdated on a daily basis, this corresponds to the everyday state update from 5 -> 3/4 -> 5.
```sh
run ...
```

#### reset
This corresponds to state 2/3/4/5 -> state 1/2 transition.
```sh
reset ...
```


## Back Testing

- Backtests are not ready to run yet.