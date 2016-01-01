#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function

import logging
logger = logging.getLogger(__name__)

import traceback

import datetime
from abc import ABCMeta, abstractmethod

import requests

import pandas as pd
from pandas.tseries.frequencies import to_offset
from pandas.core.common import is_number

from arctic.exceptions import NoDataFoundException

from ..library import idx_min, idx_max


class Updater(object):
    __metaclass__ = ABCMeta

    def __init__(self, session=None):
        self.session = session

    def start_default(self, freq):
        if freq is None:
            return datetime.datetime(2014, 1, 1)
        else:
            return self.end_default(freq) - freq * self.periods_default

    @property
    def periods_default(self):
        return 1000

    def end_default(self, freq):
        if freq == to_offset('D'):
            return (datetime.datetime.utcnow() - freq).replace(hour=0, minute=0, second=0, microsecond=0)
        elif freq is None:
            dt = datetime.datetime.utcnow()
            year = dt.year
            month = dt.month
            if month == 1:
                year -= 1
            month = (month - 2) % 12 + 1
            return datetime.datetime(year=year, month=month, day=1)
        else:
            return datetime.datetime.utcnow()

    def _sanitize_dates(self, start, end):
        """
        Return (datetime_start, datetime_end) tuple
        if start is None - default is 2010/01/01
        if end is None - default is today
        """
        if is_number(start):
            # regard int as year
            start = datetime.datetime(start, 1, 1)
        start = pd.to_datetime(start)

        if is_number(end):
            end = datetime.datetime(end, 1, 1)
        end = pd.to_datetime(end)

        if start is None:
            start = datetime.datetime(2010, 1, 1)
        if end is None:
            end = datetime.datetime.today()
        return start, end

    def _sanitize_bounds(self, symbol, start, end, freq, source, library):
        """
        Sanitize date
        """
        logger.debug("sanitize bounds %s %s %s" % (start, end, freq))
        freq = to_offset(freq)
        if start is None:
            try:
                dt_max = idx_max(library, symbol)
                if freq is not None:
                    start = dt_max + freq
                else:
                    start = dt_max + datetime.timedelta(days=1)
            except Exception as e:
                start = self.start_default(freq)
                logger.error(traceback.format_exc())
        else:
            if end is None:
                try:
                    dt_min = idx_min(library, symbol)
                    if freq is not None:
                        end = dt_min - freq
                    else:
                        end = dt_min - datetime.timedelta(days=1)
                except Exception as e:
                    end = self.end_default(freq)
                    logger.error(traceback.format_exc())
        if end is None:
            end = self.end_default(freq)
        logger.debug("sanitized bounds %s %s %s" % (start, end, freq))
        return start, end, freq

    def set_credentials(self, *args, **kwargs):
        msg = "'%s' is not implemented in '%s'" \
            % ('set_credentials', self.__class__.__name__)
        raise NotImplementedError(msg)

    @property
    def shortname(self):
        return 'DEF_UPD'

    def library_name(self, source, freq):
        if freq is None:
            return self.shortname + '_' + source
        else:
            return self.shortname + '_' + source + '_' + str(freq)

    def _update(self, library, symbol, start, end, freq, source):
        logger.info("update '%s' using '%s' with source='%s' from '%s' to '%s'" % (
            symbol, self.__class__.__name__, source, start, end))
        df_new = self.read(symbol, start, end, freq, source)
        try:
            df_prev = library.read(symbol).data
            df = pd.concat([df_prev, df_new], verify_integrity=True)
            df = df.sort_index()
        except NoDataFoundException:
            df = df_new
        logger.info("\n%s" % df)
        metadata = {
            'source': source,
            #'freq': str(freq),
            'start': df.index.min(),
            'end': df.index.max()
        }
        library.write(symbol, df, metadata=metadata)

    def update(self, library, symbol, start, end, freq, source):
        """
        Update data to library

        start : string or datetime-like, default None
            Left bound for generating dates
        end : string or datetime-like, default None
            Right bound for generating dates
        periods : integer or None, default None
            If None, must specify start and end
        """

        start, end, freq = self._sanitize_bounds(symbol, start, end, freq, source, library)
        msg = "%s %s %s" % (start, end, freq)
        logger.debug(msg)
        if start is None or end is None:
            self._update(library, symbol, start, end, freq, source)
        elif start < end:
            self._update(library, symbol, start, end, freq, source)
        elif start - freq == end:
            logger.debug("start=%s end=%s - can't update" % (start, end))
        elif start is not None and start == end:
            logger.debug("start==end==%s" % start)
        else:
            msg = "start=%s end=%s 'end' should be after 'start'" % (start, end)
            raise NotImplementedError(msg)

    def _init_session(self, session, retry_count):
        if session is None:
            session = requests.Session()
        return session
