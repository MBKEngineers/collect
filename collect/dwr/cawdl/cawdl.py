"""
collect.dwr.cawdl.cawdl
============================================================
access CA Water Data Library surface water and well data
"""
# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import pandas
import requests
import re
import os


def get_cawdl_data(site_id): # NEEDS UPDATES
    """
    Download well timeseries data from CAWDL database; return as dataframe

    Arguments:
        site_id (str): string representing cawdl site id; ie '17202'
    Returns:
        dictionary: dictionary of 'data' and 'info' with dataframe of timeseries and station metadata
    """
    cawdl_url = 'http://wdl.water.ca.gov/waterdatalibrary/groundwater/hydrographs/'
    table_url = cawdl_url + 'report_xcl_brr.cfm?CFGRIDKEY={0}&amp;type=xcl'.format(site_id)
    site_url = cawdl_url + 'brr_hydro.cfm?CFGRIDKEY={0}'.format(site_id)

    # read historical ground water timeseries from "recent groundwater level data" tab
    df = pandas.read_csv(table_url, header=2, skiprows=[1], parse_dates=[0], index_col=0)
    # df = df.tz_localize('US/Pacific')

    # parse HTML file structure; extract station/well metadata
    well_info = {}
    soup = BeautifulSoup(requests.get(site_url).content, 'html.parser')
    for table in soup.find_all('table')[1:]:
        for tr in table.find_all('tr'):
            cells = tr.find_all('td')
            if len(cells) == 2:
                key = cells[0].text.strip()
                try:
                    value = float(cells[1].text.strip())
                except ValueError:
                    value = cells[1].text.strip()
                well_info.update({key: value})

    return {'data': df, 'info': well_info}


def get_cawdl_surface_water_data(site_id, water_year, variable, interval=None):
    """
    Download timeseries data from CAWDL database; return as dataframe

    Arguments:
        site_id (str): string representing cawdl site id; ie 'B94100'
        water_year (int): integer representing water year to collect data from
        variable (str): measurement description; ie 'STAGE' or 'FLOW' or 'CONDUCTIVITY' or 'WATER_TEMPERATURE'
        interval (str): measurement time interval; ie '15-MINUTE_DATA' or 'DAILY_MEAN' or 'DAILY_MINMAX' or 'POINT' (default for conductivity & temp)
    Returns:
        dictionary: dictionary of 'data' and 'info' with dataframe of timeseries and station metadata
    """
    # cawdl_url = 'http://wdl.water.ca.gov/waterdatalibrary/docs/Hydstra/'
    cawdl_url = 'https://wdlstorageaccount.blob.core.windows.net/continuousdata/'

    if not interval and variable in ('CONDUCTIVITY', 'WATER_TEMPERATURE'):
        interval = 'POINT'

    table_url = cawdl_url + 'docs/{0}/{1}/{2}_{3}_DATA.CSV'.format(site_id, water_year, variable, interval)
    site_url = cawdl_url + 'index.cfm?site={0}'.format(site_id) # HAVE TO CHANGE AND ADD TO SITE INFO

    # read historical ground water timeseries from "recent groundwater level data" tab
    df = pandas.read_csv(table_url, header=[0, 1, 2], parse_dates=[0], index_col=0)
    df.index.name = ' '.join(df.columns.names)
    sensor_meta = df.columns[0]
    if interval == 'DAILY_MINMAX':
        n = 7
    else:
        n = 3
    df.columns = [df.columns[i][2] for i in range(n)]
    meta = df[f'Unnamed: {n}_level_2'].dropna()
    df.drop(f'Unnamed: {n}_level_2', axis=1, inplace=True)
    # df = df.tz_localize('US/Pacific')

    site_info = get_cawdl_surface_water_site_report(site_id)['info']

    return {'data': df, 'info': site_info}

def get_cawdl_surface_water_por(site_id, variable, interval=None):
    """
    Download full POR timeseries from CAWDL database

    Arguments:
        site_id (str): string representing cawdl site id; ie 'B94100'
        water_year (int): integer representing water year to collect data from
        variable (str): measurement description; ie 'STAGE' or 'FLOW' or 'CONDUCTIVITY' or 'WATER_TEMPERATURE'
        interval (str): measurement time interval; ie '15-MINUTE_DATA' or 'DAILY_MEAN' or 'DAILY_MINMAX' or 'POINT' (default for conductivity & temp)
    Returns:
        dictionary: dictionary of 'data' and 'info' with dataframe of timeseries and station metadata
    """
    return get_cawdl_surface_water_data(site_id, 'POR', variable, interval)

def get_cawdl_surface_water_site_report(site_id):
    """
    Download site report from CAWDL database

    Arguments:
        site_id (str): string representing cawdl site id; ie 'B94100'
    Returns:
        dictionary: 'info' key with text from CAWDL site report
    """
    cawdl_url = 'https://wdlstorageaccount.blob.core.windows.net/continuousdata/'
    report_url = cawdl_url + 'docs/{0}/POR/Site_Report.txt'.format(site_id)

    # parse HTML file structure; extract station/well metadata
    site_info = {}
    file = requests.get(report_url)

    site_info['available series'] = []
    start_index = 9999
    end_index = 9999

    for i, line in enumerate(file):
        if i > 5: # skip title lines
            try:
                decoded_line = line.decode().strip()

                if 'Comment' in decoded_line:
                    end_index = i
                if 'Variable' in decoded_line:
                    start_index = i

                if i <= end_index: 
                    if ':' in decoded_line:
                        key = decoded_line.split(':')[0]
                        value = decoded_line.split(':')[1]
                        value =  os.linesep.join([s for s in value.splitlines() if s]).strip()
                        site_info.update({key: value})

                if i > start_index:
                    series = decoded_line.split('  ')[0]
                    if decoded_line != '':
                        site_info['available series'].append(series)

            except UnicodeDecodeError:
                pass

    return {'info': site_info}
