#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
from arctic_updater.defaults import (MONGO_HOST_DEFAULT, \
    SOURCE_DEFAULT, FREQ_DEFAULT, SYMBOL_DEFAULT, UPDATER_DEFAULT)

def main():
    parser = argparse.ArgumentParser(prog="store", description='Store data to DB')
    parser.add_argument('--host', help="MongoDB host", default=MONGO_HOST_DEFAULT, type=str)
    parser.add_argument('--updater', help="Updater", default=UPDATER_DEFAULT, type=str)
    parser.add_argument('-s', '--source', help="Source", default=SOURCE_DEFAULT, type=str)
    parser.add_argument('--symbol', help="Symbol", default=SYMBOL_DEFAULT, type=str)
    parser.add_argument('--start', help="Start date", default='', type=str)
    parser.add_argument('--end', help="End date", default='', type=str)
    parser.add_argument('--freq', help="Freq", default='D', type=str)
    parser.add_argument('--max_rows', help="max_rows", default=10, type=int)
    parser.add_argument('--max_columns', help="max_columns", default=6, type=int)
    parser.add_argument('--api_key', help="API key", default='', type=str)
    parser.add_argument('--expire_after', help="Cache expiration ('0': no cache, '-1': no expiration, 'HH:MM:SS.X': expiration duration)", default='24:00:00.0', type=str)
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
    my_updater = updater(args.updater, session=session)
    if args.api_key != '':
        my_updater.set_credentials(api_key=args.api_key)

    store = Arctic(args.host)
    library_name = args.updater.lower() + '_' + args.source.lower()
    print(library_name)
    store.initialize_library(library_name)
    library = store[library_name]

    update(library, my_updater, args.symbol, start, end, freq, args.source.lower())


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
