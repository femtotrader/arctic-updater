#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function

import logging
logger = logging.getLogger(__name__)

import pandas_datareader.data as web
from .base import Updater

class PandasDataReaderUpdater(Updater):
    shortname = 'PDR'

    def __init__(self, *args, **kwargs):
        """
        session : requests.Session or requests_cache.CachedSession
        """
        super(PandasDataReaderUpdater, self).__init__(*args, **kwargs)

    def read(self, symbol, start, end, freq, source):
        """
        symbol : 
        freq : string or DateOffset, default 'D' (calendar daily)
            Frequency strings can have multiples, e.g. '5H'
        source :
        """
        df = web.DataReader(symbol, source, start, end, session=self.session)
        df.index = df.index.tz_localize('UTC')  # see https://github.com/pydata/pandas-datareader/issues/154
        return df

    def set_credentials(self, *args, **kwargs):
        msg = "'%s' is not implemented in '%s'" \
            % ('set_credentials', self.__class__.__name__)
        raise NotImplementedError(msg)
