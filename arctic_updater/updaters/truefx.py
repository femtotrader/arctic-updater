#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function

import logging
logger = logging.getLogger(__name__)

import traceback

import os
import datetime
import requests

import pandas as pd
import pandas.compat as compat
from pandas.io.common import ZipFile
from pandas.tseries.frequencies import to_offset

from .base import Updater

#from clint.textui import progress  # pip install clint

class TrueFXUpdater(Updater):
    shortname = 'TRUEFX'

    """Get data from TrueFX"""

    def __init__(self, retry_count=3, pause=0.001, session=None, cache_directory='~/cache/truefx/'):
        if not isinstance(retry_count, int) or retry_count < 0:
            raise ValueError("'retry_count' must be integer larger than 0")
        self.retry_count = retry_count
        self.pause = pause
        self.session = self._init_session(session, retry_count)
        self.cache_directory = os.path.expanduser(cache_directory)

    def url(self, symbol, year, month):
        month_name = datetime.datetime(year=year, month=month, day=1).strftime('%B').upper()
        return 'http://www.truefx.com/dev/data/{year:04d}/{month_name}-{year:04d}/{symbol}-{year:04d}-{month:02d}.zip'.format(year=year, month=month, symbol=symbol, month_name=month_name)

    def _sanitize_symbol(self, symbol):
        return symbol.replace("/", "").upper()

    def _filename(self, symbol, year, month, ext):
        return "{symbol}-{year:04d}-{month:02d}{ext}".format(year=year, month=month, symbol=symbol, ext=ext)

    @property
    def symbols(self):
        return ["AUDJPY", "AUDNZD", "AUDUSD", "CADJPY", "CHFJPY", \
            "EURCHF", "EURGBP", "EURJPY", "EURUSD", "GBPJPY", "GBPUSD", \
            "NZDUSD", "USDCAD", "USDCHF", "USDJPY"]

    def start_default(self, freq):
        return datetime.datetime(2009, 5, 1)

    def end_default(self, freq):
        dt = datetime.datetime.utcnow()
        dt -= datetime.timedelta(days=60)
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    def read(self, symbols, start, end, freq, source):
        """ read data """
        assert source == 'ticks' and freq is None
        start, end = self._sanitize_dates(start, end)
        # If a single symbol, (e.g., 'GOOG')
        if isinstance(symbols, (compat.string_types, int)):
            df = self._read_several_months(symbols, start, end)
        # Or multiple symbols, (e.g., ['GOOG', 'AAPL', 'MSFT'])
        else:
            raise NotImplementedError("Can't download several symbols")
        return df


    def _ym_to_int(self, year, month):
        return year * 12 + month - 1

    def _int_to_ym(self, i):
        year, month = divmod(i, 12)
        month += 1
        return year, month

    def _year_month_generator(self, start, end, reversed=False):
        logger.debug("month generator from %s to %s" % (start, end))
        months = pd.date_range(start=start, end=end, freq='MS')  # ToDo: use _ym_to_int and _int_to_ym instead?
        if reversed:
            _months = months[::-1]
        else:
            _months = months
        for dt in _months:
            yield dt.year, dt.month

    def _get(self, symbol, year, month, filename_cache, as_):
        url = self.url(symbol, year, month)
        if os.path.isfile(filename_cache) and os.path.getsize(filename_cache) > 0:
            logger.debug("loading file '%s'" % filename_cache)
            fd = open(filename_cache, 'r')
            from_file_cache = True
            return fd, from_file_cache
        else:
            logger.debug("querying url '%s'" % url)
            response = self.session.get(url)

            response = self.session.get(url)
            #response = self.session.get(url, stream=True)
            #total_length = int(response.headers.get('content-length'))
            #for chunk in progress.bar(response.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1): 
            #    pass
            if not response.status_code == requests.codes.ok:
                msg = "status_code is %d instead of %d" % (response.status_code, requests.codes.ok)
                raise NotImplementedError(msg)
            from_file_cache = False
            if as_ == 'bytes':
                data = compat.BytesIO(response.content)
            elif as_ == 'string':
                data = compat.StringIO(response.content)
            else:
                data = response.content
            return data, from_file_cache

    def download(self, symbol, year, month):
        filename_cache = os.path.join(self.cache_directory, self._filename(symbol, year, month, '.zip'))
        fd, from_file_cache = self._get(symbol, year, month, filename_cache, 'raw')
        if not from_file_cache:
            with open(filename_cache, 'w') as fd_cache:
                fd_cache.write(fd)

    def _read_one_month(self, symbol, year, month):
        symbol = symbol.replace("/", "").upper()
        filename_cache = os.path.join(self.cache_directory, self._filename(symbol, year, month, '.zip'))
        zip_data, from_file_cache = self._get(symbol, year, month, filename_cache, 'bytes')

        with ZipFile(zip_data, 'r') as zf:
            zfile = zf.open(self._filename(symbol, year, month, '.csv'))
            df = pd.read_csv(zfile, names=['Symbol', 'Date', 'Bid', 'Ask'])

        df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d %H:%M:%S.%f')
        df = df.set_index('Date')
    
        return df

    def _read_several_months(self, symbol, start, end):
        symbol = self._sanitize_symbol(symbol)
        lst = []
        lst_errors = []
        for year, month in self._year_month_generator(start, end):
            try:
                df = self._read_one_month(symbol, year, month)
                lst.append(df)
            except KeyError:
                pass
            except Exception as e:
                lst_errors.append((symbol, year, month))
                logger.error(traceback.format_exc())
        df_all = pd.concat(lst)
        return df_all
