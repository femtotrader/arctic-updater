#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function

import logging
logger = logging.getLogger(__name__)

import requests_cache

import pandas as pd
from collections import OrderedDict

def get_session(expire_after, cache_name='cache'):
    """
    Returns a `requests.Session` or a `requests_cache.CachedSession`

    Parameters
    ----------
    expire_after : `str`    
        cache expiration delay
                    '-1' : no cache
                     '0' : no expiration
            '00:15:00.0' : expiration delay

    cache_filename : `str`
        Name of cache file

    """
    if expire_after=='-1':
        expire_after = None
        logger.debug("expire_after==0 no cache")
        session = requests.Session()
    else:
        if expire_after=='0':
            expire_after = 0
            logger.debug("Installing cache '%s.sqlite' without expiration" % cache_name)
        else:
            expire_after = pd.to_timedelta(expire_after, unit='s')
            logger.debug("Installing cache '%s.sqlite' with expire_after=%s (d days hh:mm:ss)" % (cache_name, expire_after))
        session = requests_cache.CachedSession(\
            cache_name=cache_name, expire_after=expire_after)
    return session

def tablename_to_dict(tablename):
    """
    >>> tablename_to_dict('OHLCV_D_PDR_YAHOO_AAPL')
    OrderedDict([('store', 'OHLCV'), ('freq', 'D'), ('updater', 'PDR'), ('source', 'YAHOO'), ('symbol', 'AAPL')])
    """
    lst = tablename.split('_')
    store, freq, updater_shortname, source, symbol = lst
    d = OrderedDict([
        ('store', store),
        ('freq', freq),
        ('updater', updater_shortname),
        ('source', source),
        ('symbol', symbol)
    ])
    return d

def main():
    import doctest
    doctest.testmod()

if __name__ == '__main__':
    main()