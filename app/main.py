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
import argparse
from datetime import date

from loguru import logger
from dotenv import load_dotenv

from app.constant.version import VERSION
from app.constant.schedule import previous_trade_day
from app.db.engine import engine_from_env
from app.utils.ingest import auto_fill
from app.utils.update import calculate_ma250
from app.utils.filter import filter_desired


load_dotenv()
logger.remove()
logger.add(sys.stdout, level=os.environ.get("LOG_LEVEL", "INFO"))



def build_parser():
    parser = argparse.ArgumentParser(
        prog="stock-picker",
        description='Setup database or run the stock picker',
    )
    parser.add_argument('--version', action='version', version=f'%(prog)s {VERSION}')
    subparsers = parser.add_subparsers(dest="subcommand_name", help='subcommand help')

    #
    # initialize database    
    subparser_init = subparsers.add_parser('init', 
                                           help='Initialize the database'
    )
    subparser_init.add_argument('-d', '--dry-run', action='store_true', default=False, help='Show table schema without initializing the database')
    subparser_init.add_argument('-f', '--force', action='store_true', default=False, help='Force initialization of the database, resetting and dropping any existing data')
    subparser_init.add_argument('-l', '--load', action='store_true', default=False, help='Load all historical data after initialization')

    #
    # run tasks
    subparser_run = subparsers.add_parser('run',
                                          help='Run the stock picker, including refreshing stock data, calculating moving averages, and filtering desired stocks'
    )
    subparser_run.add_argument('--date', default=previous_trade_day(date.today()), help='The trade day to run the stock picker for')
    subparser_run.add_argument('-d', '--dryrun', action='store_true', default=False , help='Show task run results without committing')
    subparser_run.add_argument('-t', '--task', default='all', help='The trade task to run the stock picker for')

    #
    # reset database
    subparser_reset = subparsers.add_parser('reset', 
                                            help='Reset the database to the initial/clean state'
    )
    subparser_reset.add_argument('-b', '--backup',  help='Back up the database by dumping')
    subparser_reset.add_argument('-d', '--dryrun', action='store_true', default=False , help='Show the tables to be reset without actually resetting them')
    subparser_reset.add_argument('-i', '--init', action=argparse.BooleanOptionalAction, default=True, help='Init after purge')
    subparser_reset.add_argument('-y', '--yes', action='store_true', default=False , help='Say yes to reset')

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    logger.debug(f'Parsed args = {vars(args)}')
    
    engine = engine_from_env()

    match args.subcommand_name:
        case 'init':
            print('Initializing the database')
            if args.force:
                logger.error("Not implmented!")
            if args.dry_run:
                logger.error("Not implmented!")
            if args.load:
                logger.error("Not implmented!")


        case 'run':                
            trade_day = args.date
            task = args.task.strip()
            dryrun = args.dryrun

            match task:
                case "ingest":
                    auto_fill(engine, trade_day)
                case "update":
                    calculate_ma250(engine, trade_day, dryrun=dryrun)
                case "filter":
                    df = filter_desired(engine, trade_day, dryrun=dryrun)
                    df.to_csv(f'reports/report-{trade_day}.csv')
                    df.to_excel(f'reports/report-{trade_day}.xlsx')

                case "display":
                    logger.error("Not implmented!")

                case 'all':
                    auto_fill(engine, trade_day)
                    calculate_ma250(engine, trade_day)
                    df = filter_desired(engine, trade_day, dryrun=dryrun)
                    
                    if not os.path.exists('reports'):
                        os.makedirs('reports')
                    df.to_csv(f'reports/report-{trade_day}.csv')
                    df.to_excel(f'reports/report-{trade_day}.xlsx')
                    logger.info("output under reports/")

                case _:
                    logger.error(f"Unknown task: {task}")

        case 'reset':
            print('Resetting the database')
            if args.yes:
                logger.error("Not implmented!")


        case _:
            assert False, 'should not reach'


if __name__ == "__main__":
    main()
