#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
python arctic_updater/cli/cache_download.py --updater truefx --symbols "" --start 20090501 --end 20151101
"""

import logging
logger = logging.getLogger(__name__)

import argparse

import os
import datetime

import pandas as pd

from arctic import Arctic

from arctic_updater import arctic_updater

from arctic_updater.updaters.factory import updater
from arctic_updater.library import update
from arctic_updater.utils import get_session
from arctic_updater.defaults import (\
    FREQ_DEFAULT, SYMBOLS_DEFAULT, UPDATER_DEFAULT)

def main():
    parser = argparse.ArgumentParser(prog="download", description='')
    parser.add_argument('--updater', help="Updater", default='truefx', type=str)
    parser.add_argument('--symbols', help="Symbol", default='EURUSD', type=str)
    parser.add_argument('--start', help="Start date", default='', type=str)
    parser.add_argument('--end', help="End date", default='', type=str)
    parser.add_argument('--freq', help="Freq", default='', type=str)
    parser.add_argument('--max_rows', help="max_rows", default=10, type=int)
    parser.add_argument('--max_columns', help="max_columns", default=6, type=int)
    parser.add_argument('--api_key', help="API key", default='', type=str)
    parser.add_argument('--expire_after', help="Cache expiration ('0': no cache, '-1': no expiration, 'HH:MM:SS.X': expiration duration)", default='24:00:00.0', type=str)
    parser.add_argument('--cache_directory', help="Cache directory", default='~/cache/truefx/', type=str)
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)
    pd.set_option('max_rows', args.max_rows)
    pd.set_option('expand_frame_repr', False)
    pd.set_option('max_columns', args.max_columns)

    if args.start != '':
        start = pd.to_datetime(args.start)
    else:
        start = None

    if args.end != '':
        end = pd.to_datetime(args.end)
    else:
        end = None

    if args.freq != '':
        freq = args.freq
    else:
        freq = None

    
    session = get_session(args.expire_after, 'cache')
    my_updater = updater(args.updater, session=session, cache_directory=args.cache_directory)
    if args.api_key != '':
        my_updater.set_credentials(api_key=args.api_key)

    if args.symbols != '':
        symbols = args.symbols.split(',')
    else:
        symbols = my_updater.symbols


    logger.info(args)
    logger.info(my_updater)
    logger.info(symbols)
    start, end = my_updater._sanitize_dates(start, end)
    logger.info(start)
    logger.info(end)
    for year, month in my_updater._year_month_generator(start, end):
        for symbol in symbols:
            print(symbol, year, month)
            my_updater.download(symbol, year, month)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
