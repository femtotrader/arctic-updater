#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function

import logging
logger = logging.getLogger(__name__)

import traceback

import datetime

from pandas.tseries.frequencies import to_offset

from abc import ABCMeta, abstractmethod

from ..library import idx_min, idx_max

class Updater(object):
    __metaclass__ = ABCMeta

    #shortname = 'DEF_UPD'

    def __init__(self, session=None):
        self.session = session

    def start_default(self, freq):
        #return datetime.datetime(2012, 1, 1)
        return self.end_default(freq) - freq * self.periods_default

    @property
    def periods_default(self):
        return 1000

    def end_default(self, freq):
        if freq == to_offset('D'):
            return (datetime.datetime.utcnow() - freq).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            return datetime.datetime.utcnow()

    def _sanitize_bounds(self, symbol, start, end, freq, source, library):
        """
        Sanitize date
        """
        logger.debug("sanitize bounds %s %s %s" % (start, end, freq))
        freq = to_offset(freq)
        if start is None:
            try:
                dt_max = idx_max(library, symbol)
                start = dt_max + freq
            except Exception as e:
                start = self.start_default(freq)
                logger.error(traceback.format_exc())
        else:
            if end is None:
                try:
                    dt_min = idx_min(library, symbol)
                    end = dt_min - freq
                except Exception as e:
                    end = self.end_default(freq)
                    logger.error(traceback.format_exc())
        if end is None:
            end = self.end_default(freq)
        logger.debug("sanitized bounds %s %s %s" % (start, end, freq))
        return start, end, freq

    @abstractmethod
    def set_credentials(self, *args, **kwargs):
        msg = "'%s' is not implemented in '%s'" \
            % ('set_credentials', self.__class__.__name__)
        raise NotImplementedError(msg)

    @property
    def shortname(self):
        return 'DEF_UPD'

    def _update(self, library, symbol, start, end, freq, source):
        logger.info("update '%s' using '%s' with source='%s' from '%s' to '%s'" % (
            symbol, self.__class__.__name__, source, start, end))
        df = self.read(symbol, start, end, freq, source)
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
