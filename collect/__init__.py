"""
collect
============================================================
The core module of MBK Engineers' Folsom Reservoir Flood Operations Model
"""
# -*- coding: utf-8 -*-
__all__ = ['alert', 'cnrfc', 'cvo', 'dwr', 'ewrims', 'noaa', 'nrcs', 
           'srwp', 'telemetry', 'usace', 'usgs', 'utils']
__author__ = 'MBK Engineers'
__docs__ = 'collect: webscrapers for water resources'

from . import alert, cnrfc, cvo, dwr, ewrims, noaa, nrcs, srwp, telemetry, usace, usgs, utils
