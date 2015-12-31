#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function

import logging
logger = logging.getLogger(__name__)

import Quandl
from .base import Updater

class QuandlUpdater(Updater):
    shortname = 'QUANDL'

    def __init__(self, *args, **kwargs):
        """
        session : requests.Session or requests_cache.CachedSession
        """
        super(QuandlUpdater, self).__init__(*args, **kwargs)
        self.api_key = ''

    def start_default(self, freq):
        return None

    def end_default(self, freq):
        return None

    def set_credentials(self, api_key=''):
        self.api_key = api_key

    def read(self, symbol, start, end, freq, source):
        """
        symbol : 
        freq : string or DateOffset, default 'D' (calendar daily)
            Frequency strings can have multiples, e.g. '5H'
        source :
        """
        if source != '':
            code = "/".join([source, symbol])
        else:
            code = symbol
        df = Quandl.get(code, authtoken=self.api_key, trim_start=start, trim_end=end)
        return df
