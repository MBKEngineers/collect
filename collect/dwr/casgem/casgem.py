"""
collect.dwr.casgem.casgem
============================================================
access CASGEM well data
"""
# -*- coding: utf-8 -*-


def get_casgem_data(casgem_id=None, state_well_number=None, local_well_designation=None, master_site_code=None):
    """
    download well timeseries data from CASGEM database; return timeseries data as dataframe and include site meta data

    Arguments:
        casgem_id (str): string identifier used by CASGEM to id a well, i.e. '34318'
        state_well_number (None): identifier from state well numbering system, i.e. '19N02W36H001M'
        local_well_designation (None): local well name, i.e. '19N02W36H001M'
        master_site_code (None): identifier from alternate numbering system, i.e. '394564N1220246W001'
    
    Raises:
        NotImplementedError: prior CASGEM web scraper function obsolete due to DWR CASGEM website schema change
    """
    raise NotImplementedError('CASGEM web scraper contingent on completion of DWR CASGEM website schema change')
