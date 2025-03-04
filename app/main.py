"""
Stock Picker

This application is designed to set up and run a stock picker that refreshes stock data,
calculates moving averages, filters desired stocks based on certain criteria, and 
displays them into configured output based on .env file.

Since the applciation is designed to be run from different states of the database, it
needs to be run in a specific order using different commands to ensure that the data is
up-to-date and correct.

Usage:
    Run this script to set up or run the stock picker.

Example:
    PYTHONPATH=. python 

Author: monkeyboiii
Date: 2025-02-01
License: MIT
"""

import os
import sys
import json
import argparse
from datetime import date, datetime, time

from loguru import logger
from dotenv import load_dotenv

from app.backtest.feed import refresh_feed_daily_table
from app.constant.exchange import MARKET_SUPPORTED
from app.constant.version import VERSION
from app.constant.schedule import previous_trade_day
from app.db.engine import engine_from_env
from app.db.load import (
    load_market, 
    load_all_stocks, 
    load_default_collections, 
    load_collection_stock_relation,
    load_by_level,
)
from app.db.materialized_view import (
    check_mv_exists, 
    check_mv_procedure_exists,
    daily_recreate_mv,
)
from app.db.models import FeedDaily
from app.display.tdx import add_to_tdx_path
from app.display.google_sheet import add_df_to_new_sheet
from app.filter.tail_scraper import filter_desired
from app.utils.ingest import auto_fill
from app.utils.update import calculate_ma250
from app.utils.reset import reset_db_content


load_dotenv(override=True)


def build_parser():
    parser = argparse.ArgumentParser(prog="stock-picker",
                                    description='Setup database or run the stock picker',
    )
    parser.add_argument('-q', '--quiet', action='store_true', default=False, help='Supress any logs below SUCCESS, inclusive')
    parser.add_argument('-s', '--supress', action='store_true', default=False, help='Supress any logs below WARNING, inclusive')
    parser.add_argument('-S', '--store-log', action='store_true', default=False, help='Store full logs to a seperate file')
    parser.add_argument('-t', '--trace', action='store_true', default=False, help='Store tracing logs to a seperate file')
    parser.add_argument('-v', '--verbose', action='count', default=0, help='Increase verbosity, default at SUCCESS')
    parser.add_argument('-V', '--version', action='version', version=f'%(prog)s {VERSION}')
    subparsers = parser.add_subparsers(dest="subcommand_name", help='subcommand help')

    #
    # initialize database    
    subparser_init = subparsers.add_parser('init', 
                                           help='Initialize the database'
    )
    subparser_init.add_argument('-d', '--dryrun', action='store_true', default=False, help='Show table schema without initializing the database')
    subparser_init.add_argument('-e', '--echo', action='store_true', default=False, help='Echoing DDLs')
    subparser_init.add_argument('-i', '--ingest', action='store_true', default=False, help='Ingest all historical data after load')
    subparser_init.add_argument('-l', '--load', action='count', default=0, help='Load 0: none, 1: market, 2: stocks, 3: collection data after initialization')
    subparser_init.add_argument('-r', '--reset', action='store_true', default=False, help='Reinitialization of the database by dropping any all tables')
    subparser_init.add_argument('-y', '--yes', action='store_true', default=False, help='Say yes to reset, use with caution')

    #
    # run tasks
    subparser_run = subparsers.add_parser('run',
                                          help='Run the stock picker, including refreshing stock data, calculating moving averages, and filtering desired stocks'
    )
    subparser_run.add_argument('--date', default=date.today().isoformat(), help='The trade day to run the stock picker for')
    subparser_run.add_argument('-l', '--load', nargs='?', default='all', help='To load market/stock/collection/all (semi-)static data')
    subparser_run.add_argument('-d', '--dryrun', action='store_true', default=False, help='Show task run results without committing, only applies to update/filter tasks')
    subparser_run.add_argument('-s', '--skip', action='store_true', default=False, help='Skip autof fill history, if you are confident they are correct')
    subparser_run.add_argument('-m', '--materialized', action=argparse.BooleanOptionalAction, default=True, help='Recreate/create materialized view')
    subparser_run.add_argument('-t', '--task', default='all', help='The trade task to run the stock picker for')
    subparser_run.add_argument('-y', '--yes', action='store_true', default=False, help='Say yes to confirms')

    #
    # reset tables
    # TODO reset with backup, or for specific tables
    subparser_reset = subparsers.add_parser('reset', 
                                            help='Reset the database to the initial/clean state'
    )
    subparser_reset.add_argument('-b', '--backup',  help='Back up the database by dumping')
    subparser_reset.add_argument('-d', '--dryrun', action='store_true', default=False, help='Show the tables to be reset without actually resetting them')
    subparser_reset.add_argument('-i', '--init', action=argparse.BooleanOptionalAction, default=True, help='Init after purge')
    subparser_reset.add_argument('-t', '--table',  help='Drop and create one table')
    subparser_reset.add_argument('-y', '--yes', action='store_true', default=False, help='Say yes to reset')

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    # configure log level
    logger.remove()

    if args.trace:
        logger.add("tracing.log", level='TRACE', filter=lambda r: r['level'].name == 'TRACE')
        logger.trace("-------------------- start tracing --------------------")
    if args.store_log:
        logger.add("full.log", level='DEBUG')
        
    if args.supress:
        logger.add(sys.stdout, level="ERROR")
    elif args.quiet:
        logger.add(sys.stdout, level="WARNING")
    else:
        match args.verbose:
            case 0:
                logger.add(sys.stdout, level="SUCCESS")
            case 1:
                logger.add(sys.stdout, level="INFO")
            case 2:
                logger.add(sys.stdout, level="DEBUG")
            case _:
                logger.add(sys.stdout, level="TRACE")
    logger.debug(f'Parsed args =\n{json.dumps(vars(args), sort_keys=True, indent=4)}')

    # 
    match args.subcommand_name.strip():
        
        ################################################################################
        case 'init':
            dryrun = args.dryrun
            engine = engine_from_env(echo=args.echo)

            reset_db_content(
                engine=engine,
                reset=args.reset, 
                dryrun=dryrun,
                yes=args.yes,
            )
            
            if not dryrun and args.load:
                # load 
                # none 0
                # market 1
                # stocks 2
                # collections 3
                load_by_level(engine=engine, level=args.load)

                if args.ingest:
                    raise Exception("Not implemented yet!")
        
        ################################################################################
        case 'run':                
            engine = engine_from_env()
            
            # args
            trade_day = previous_trade_day(date.fromisoformat(args.date))
            task = args.task.strip()
            dryrun = args.dryrun

            match task:
                ############################
                case "load":
                    match args.load:
                        case "market":
                            load_market(engine)
                        case "stock":
                            for market_name in MARKET_SUPPORTED:
                                load_all_stocks(engine, market_name)
                        case "collection":
                            dcts = load_default_collections(engine)
                            for cType in dcts:
                                load_collection_stock_relation(engine, cType)
                        case "all":
                            load_by_level(engine=engine, level=3)
                        case _:
                            logger.error(f"Unrecoginized load target {args.load}")
                
                ############################
                case "ingest":
                    auto_fill(
                        engine=engine, 
                        up_to_date=trade_day, 
                        skip_hist_fill=args.skip, 
                        yes=args.yes
                    )

                ############################
                case "update":
                    if args.materialized and check_mv_procedure_exists(engine):
                        if check_mv_exists(engine, trade_day, previous=True):
                            daily_recreate_mv(engine=engine, trade_day=trade_day, previous=True)
                        
                        # FIXME: change to market specific close time
                        now = datetime.now().time()
                        if now > time(15, 0) and not check_mv_exists(engine, trade_day, previous=False):
                            _ = daily_recreate_mv(engine, trade_day, previous=False)

                    calculate_ma250(
                        engine=engine, 
                        trade_day=trade_day, 
                        dryrun=dryrun
                    )

                ############################
                case "filter":
                    fds = filter_desired(
                        engine=engine, 
                        trade_day=trade_day,
                    )
                    if not dryrun:
                        df = refresh_feed_daily_table(
                            engine=engine,
                            fds=fds,
                            trade_day=trade_day,
                        )
                    else:
                        if not os.path.exists('reports'):
                            os.makedirs('reports')
                        df.to_csv(f'reports/report-{trade_day}.csv')
                        df.to_excel(f'reports/report-{trade_day}.xlsx')

                ############################
                case "display":
                    fds = filter_desired(
                        engine=engine, 
                        trade_day=trade_day
                    )
                    df = FeedDaily.to_dataframe(fds)
                    add_to_tdx_path(
                        engine=engine,
                        df=df,
                        trade_day=trade_day,
                    )
                    add_df_to_new_sheet(
                        trade_day=trade_day, 
                        df=FeedDaily.convert_to_feed(df),
                        yes=args.yes
                    )

                ############################
                case 'all':
                    # ingest: assumes load is ready
                    auto_fill(
                        engine=engine, 
                        up_to_date=trade_day, 
                        skip_hist_fill=args.skip, 
                        yes=args.yes
                    )

                    # update
                    using_mv = False
                    if args.materialized and check_mv_procedure_exists(engine):
                        if check_mv_exists(engine, trade_day, previous=True):
                            using_mv = True
                        else:
                            using_mv = daily_recreate_mv(engine=engine, trade_day=trade_day, previous=True)
                            
                            # FIXME: change to market specific close time
                            now = datetime.now().time()
                            if now > time(15, 0) and not check_mv_exists(engine, trade_day, previous=False):
                                _ = daily_recreate_mv(engine, trade_day, previous=False)
                    if not using_mv:
                        calculate_ma250(
                            engine=engine, 
                            trade_day=trade_day
                        )
                    
                    # filter
                    fds = filter_desired(
                        engine=engine, 
                        trade_day=trade_day,
                    )
                    if not dryrun:
                        df = refresh_feed_daily_table(
                            engine=engine,
                            fds=fds,
                            trade_day=trade_day,
                        )
                    else:
                        df = FeedDaily.to_dataframe(fds)

                    # display
                    add_df_to_new_sheet(
                        trade_day=trade_day, 
                        df=df,
                        yes=args.yes
                    )
                    add_to_tdx_path(
                        engine=engine,
                        trade_day=trade_day
                    )

                ############################
                case _:
                    logger.error(f"Unknown task: {task}")

        ################################################################################
        case 'reset':
            raise Exception("Not implemented yet!")


        case _:
            assert False, 'should not reach'


if __name__ == "__main__":
    main()
