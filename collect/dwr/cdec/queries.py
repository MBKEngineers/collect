"""
collect.dwr.cdec.queries
============================================================
access CDEC gage data
"""
# -*- coding: utf-8 -*-
import datetime as dt
import json
from bs4 import BeautifulSoup
import pandas as pd
import requests
from six import string_types


def get_station_url(station, start, end, data_format='CSV', sensors=[], duration=''):
    """ 
    Generate URL for CDEC station query for CSV- or JSON-formatted data 
    
    - if no start/end supplied, then 'H' defaults to last day of data
                                     'D' defaults to last month of data
                                     'M' defaults to last year of data
    - currently start and end are required arguments
    """

    # validate station(s)
    if isinstance(station, string_types): # (str for Python3 or basestring for Python2)
        if not all(len(x) == 3 for x in station.split(',')):
            raise ValueError('station code must be comma-separated `str` of 3-character CDEC station codes (i.e. \'FOL,SHA\').')

        station = ','.join([x.strip() for x in station.split(',')])
    elif isinstance(station, list) or isinstance(station, tuple):
        if not all(len(x) == 3 for x in station):
            raise ValueError('station code must be `list` or `tuple` of 3-character CDEC station codes (i.e. [\'FOL\', \'SHA\']).')
        station = ','.join([x.strip() for x in station])
    else:
        raise TypeError('station must be of type `str`, `list` or `tuple`).')

    # validate data_format
    if not data_format.upper() in ['JSON', 'CSV']:
        raise ValueError('data_format must be one of (\'JSON\', \'CSV\').')

    # validate duration
    if not (duration is None or duration.upper() in ['', 'E', 'H', 'D', 'M', 'Y']):
        raise ValueError('duration must be one of (None, \'\', \'H\', \'D\', \'M\', \'Y\').')

    # validate start and end arguments
    if not ((isinstance(start, dt.datetime) or isinstance(start, dt.date)) 
            or not (isinstance(end, dt.datetime) or isinstance(end, dt.date))):
        raise TypeError('start and end must be datetime.datetime or datetime.date objects.')

    # # ensure data through the midnight of the calendar day is retrieved
    # today = dt.datetime.now().strftime('%Y-%m-%d')
    # if isinstance(end, str):
    #     if end == today:
    #         end = dt.datetime.strptime(end, '%Y-%m-%d') + dt.timedelta(days=1)
    # elif isinstance(end, dt.datetime):
    #     if end.strftime('%Y-%m-%d') == today:
    #         end = end + dt.timedelta(days=1)

    # construct URL
    url_base = 'http://cdec.water.ca.gov/dynamicapp/req/{data_format}DataServlet'
    url_args = ['Stations={station}', 'Start={start:%Y-%m-%d}', 'End={end:%Y-%m-%d}']

    # optional sensors filter
    if bool(sensors):
        url_args.insert(1, 'SensorNums={0}'.format(','.join([str(x) for x in sensors])))

    # optional duration filter
    if bool(duration):
        url_args.insert(1, 'dur_code={0}'.format(duration))

    # construct CDEC url with query parameters
    url = '?'.join([url_base, 
                    '&'.join(url_args)]).format(data_format=data_format.upper(),
                                                station=station.upper(), 
                                                start=start, 
                                                end=end)
    return url


def get_station_sensors(station, start, end):
    """
    Returns a `dict` of the available sensors for `station` for each duration in window
    defined by `start` and `end`.
    """
    sensors = {}
    for duration in ['E', 'H', 'D', 'M']:
        url = get_station_url(station, start, end, duration=duration)
        df = pd.read_csv(url, header=0, na_values=['m', '---', ' ', 'ART', 'BRT'], usecols=[0, 1, 2, 3])
        sensors.update({duration: list(df['SENSOR_TYPE'].unique())})
    return sensors


def get_station_data(station, start, end, sensors=[], duration=''):
    """
    General purpose function for returning a pandas DataFrame for all available
    data for CDEC `station` in the given time window, with optional `duration` argument.
    """
    return get_raw_station_csv(station, start, end, sensors, duration)


def get_raw_station_csv(station, start, end, sensors=[], duration='', filename=''):
    """
    Use CDEC CSV query URL to download available data.  Optional `filename` argument
    specifies custom file location for download of CSV records.
    """
    # CDEC url with query parameters
    url = get_station_url(station, start, end, data_format='CSV', duration=duration)

    # suppress low memory error due to guessing d-types
    default_data_types = {
        'STATION_ID': str,
        'DURATION': str,
        'SENSOR_NUMBER': int,
        'SENSOR_TYPE': str,
        'DATE TIME': str,
        'OBS DATE': str,
        'VALUE': float,
        'DATA_FLAG': str,
        'UNITS': str,
    }

    # fetch data from CDEC
    df = pd.read_csv(url, 
                     header=0, 
                     parse_dates=True, 
                     index_col=4, 
                     na_values=['m', '---', ' ', 'ART', 'BRT', -9999],
                     float_precision='high',
                     dtype=default_data_types)

    if bool(filename):
        df.to_csv(filename)

    return df


def get_raw_station_json(station, start, end, sensors=[], duration='', filename=''):
    """
    Use CDEC JSON query URL to download available data.  Optional `filename` argument
    specifies custom file location for download of validated JSON records.
    """

    url = get_station_url(station, start, end, data_format='JSON', sensors=sensors, duration=duration)
    response = requests.get(url, stream=True)
    result = json.loads(response.text)

    if bool(filename):
        with open(filename, 'w') as f:
            json.dump(result, f, indent=4)

    return result


def get_sensor_frame(station, start, end, sensor='', duration=''):
    """
    return a pandas DataFrame of `station` data for a particular sensor, filtered
    by `duration` and `start` and `end` dates.
    """
    raw = get_station_data(station, start, end, duration)

    if bool(sensor) and bool(duration):
        df = raw.loc[(raw['SENSOR_NUMBER']==sensor) & (raw['DURATION']==duration)]
    elif bool(sensor):
        df = raw.loc[raw['SENSOR_TYPE']==sensor]
    else:
        raise ValueError('sensor `{}` is not valid for station `{}`'.format(sensor, station))
    
    return df


def get_station_metadata(station):
    """
    get the gage meta data and datum, monitor/flood/danger stage information
    """

    # construct URL
    url = 'http://cdec.water.ca.gov/dynamicapp/staMeta?station_id={station}'.format(station=station)

    # request info page
    soup = BeautifulSoup(requests.get(url).content, 'lxml')

    # initialize the result dictionary
    site_info = {'title':  soup.find('h2').text}
    
    # extract the station geographic info table
    table = soup.find('table')
    for tr in table.find_all('tr'):
        cells = tr.find_all('td')
        for i, cell in enumerate(cells):
            if i % 2 == 0:
                key = cells[i].text.strip()
                value = cells[i+1].text.strip()
                site_info.update({key: value})

    # extract the station datum and measurement info table
    table = soup.find_all('table')[1]
    header_row = table.find_all('tr')[0]
    data_row = table.find_all('tr')[1]

    for (k, v) in zip(header_row.find_all('th'), data_row.find_all('td')):
        key = k.text.strip()
        value = v.text.strip()
        site_info.update({key: value})

    return {'info': site_info}
