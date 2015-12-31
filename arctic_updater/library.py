#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function

import logging
logger = logging.getLogger(__name__)


def idx_min(library, symbol):
    return library.read(symbol).metadata['start']

def idx_max(library, symbol):
    return library.read(symbol).metadata['end']

def update(library, updater, symbol, start, end, freq, source):
    msg = "Update %s %s %s %s %s %s %s" % (library, updater, symbol, start, end, freq, source)
    logger.info(msg)
    updater.update(library, symbol, start, end, freq, source)

def main():
    import doctest
    doctest.testmod()

if __name__ == '__main__':
    main()
