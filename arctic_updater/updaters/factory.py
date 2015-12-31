#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function

import logging
logger = logging.getLogger(__name__)

from .base import Updater

try:
    from .quandl import QuandlUpdater
    _HAS_QUANDL = True
except ImportError:
    _HAS_QUANDL = False

try:
    from .pandas_datareader import PandasDataReaderUpdater
    _HAS_PANDAS_DATAREADER = True
except ImportError:
    _HAS_PANDAS_DATAREADER = False

try:
    from .truefx import TrueFXUpdater
    _HAS_TRUEFX = True
except ImportError:
    _HAS_TRUEFX = False

_D_CLS_UPDATER = {}
_D_CLS_UPDATER_SHORTNAME = {}
_D_NOT_INSTALLED =  {}

def updater(s, *args, **kwargs):
    s = s.lower()
    try:
        cls = _D_CLS_UPDATER[s]
    except KeyError:
        msg = "updater named '%s' is not in %s" % (s, _D_CLS_UPDATER.keys())
        if len(_D_NOT_INSTALLED) > 0:
            msg += " - not installed: %s" % _D_NOT_INSTALLED
        raise NotImplementedError(msg)
    logger.debug("using %s" % cls)
    return cls(*args, **kwargs)

def register(longname, cls):
    assert issubclass(cls, Updater), "%s must be subclass of %s" % (cls, Updater)
    _D_CLS_UPDATER[longname] = cls
    _D_CLS_UPDATER_SHORTNAME[cls.shortname] = cls

def not_installed(longname, installation=''):
    _D_NOT_INSTALLED[longname] = installation

updater_name = 'quandl'
if _HAS_QUANDL:
    register(updater_name, QuandlUpdater)
else:
    not_installed(updater_name, "pip install Quandl")

updater_name = 'pandas_datareader'
if _HAS_PANDAS_DATAREADER:
    register(updater_name, PandasDataReaderUpdater)
else:
    not_installed(updater_name, 'pip install pandas_datareader')

updater_name = 'truefx'
if _HAS_TRUEFX:
    register(updater_name, TrueFXUpdater)
else:
    not_installed(updater_name)

if len(_D_NOT_INSTALLED)>0:
    logger.warning(_D_NOT_INSTALLED)
