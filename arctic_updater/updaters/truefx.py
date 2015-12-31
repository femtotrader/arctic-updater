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


class TrueFXUpdater(Updater):
    shortname = 'TRUEFX'

    """Get data from TrueFX"""

    def __init__(self, retry_count=3, pause=0.001, session=None):
        if not isinstance(retry_count, int) or retry_count < 0:
            raise ValueError("'retry_count' must be integer larger than 0")
        self.retry_count = retry_count
        self.pause = pause
        self.session = self._init_session(session, retry_count)

    def url(self, symbol, year, month):
        month_name = datetime.datetime(year=year, month=month, day=1).strftime('%B').upper()
        return 'http://www.truefx.com/dev/data/{year:04d}/{month_name}-{year:04d}/{symbol}-{year:04d}-{month:02d}.zip'.format(year=year, month=month, symbol=symbol, month_name=month_name)

    def _sanitize_symbol(self, symbol):
        return symbol.replace("/", "").upper()

    def _filename(self, symbol, year, month, ext):
        return "{symbol}-{year:04d}-{month:02d}{ext}".format(year=year, month=month, symbol=symbol, ext=ext)

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

    def _year_month_generator(self, start, end):
        logger.debug("month generator from %s to %s" % (start, end))
        months = pd.date_range(start=start, end=end, freq='MS')
        for dt in months:
            yield dt.year, dt.month

    def _download(self, symbol, year, month, filename_cache, as_):
        url = self.url(symbol, year, month)
        if os.path.isfile(filename_cache) and os.path.getsize(filename_cache) > 0:
            logger.debug("skip '%s'" % filename_cache)
            fd = open(filename_cache, 'r')
            from_file_cache = True
            return fd, from_file_cache
        else:
            logger.debug("querying '%s'" % url)
            response = self.session.get(url)
            from_file_cache = False
            if as_ == 'bytes':
                data = compat.BytesIO(response.content)
            elif as_ == 'string':
                data = compat.StringIO(response.content)
            else:
                data = response.content
            return data, from_file_cache

    def download(self, symbol, year, month, cache_directory):
        filename_cache = os.path.join(cache_directory, self._filename(symbol, year, month, '.zip'))
        fd, from_file_cache = self._download(symbol, year, month, filename_cache, 'raw')
        if not from_file_cache:
            with open(filename_cache, 'w') as fd_cache:
                fd_cache.write(fd)

    def _read_one_month(self, symbol, year, month):
        url = self.url(symbol, year, month)
        symbol = symbol.replace("/", "").upper()

        logger.debug("querying '%s'" % url)
        response = self.session.get(url)
        if response.status_code == requests.codes.ok:
            zip_data = compat.BytesIO(response.content)

            with ZipFile(zip_data, 'r') as zf:
                zfile = zf.open(self._filename(symbol, year, month, '.csv'))
                df = pd.read_csv(zfile, names=['Symbol', 'Date', 'Bid', 'Ask'])

            df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d %H:%M:%S.%f')
            df = df.set_index('Date')
        
            return df
        else:
            logger.error("status_code is %d instead of %d" % (response.status_code, requests.codes.ok))

    def _read_several_months(self, symbol, start, end):
        symbol = self._sanitize_symbol(symbol)
        lst = []
        lst_errors = []
        for year, month in self._year_month_generator(start, end)
            try:
                df = self._read_one_month(symbol, year, month)
                lst.append(df)
            except:
                lst_errors.append((symbol, year, month))
                logger.error(traceback.format_exc())
        df_all = pd.concat(lst)
        return df_all
