# -*- coding: utf-8 -*-
import datetime as dt
import json
import pandas as pd
import requests
from six import string_types


def get_station_url(station, start, end, data_format='CSV', duration=''):
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
    if not duration.upper() in [None, '', 'E', 'H', 'D', 'M', 'Y']:
        raise ValueError('duration must be one of (None, \'\', \'H\', \'D\', \'M\', \'Y\').')

    # validate start and end arguments
    if not ((isinstance(start, dt.datetime) or isinstance(start, dt.date)) 
            or not (isinstance(end, dt.datetime) or isinstance(end, dt.date))):
        raise TypeError('start and end must be datetime.datetime or datetime.date objects.')

    # construct URL
    url_base = 'http://cdec.water.ca.gov/dynamicapp/req/{data_format}DataServlet'
    url_args = ['Stations={station}', 'Start={start:%Y-%m-%d}', 'End={end:%Y-%m-%d}']

    # optional duration filter
    if bool(duration):
        url_args = [url_args[0], 'dur_code={duration}'] + url_args[1:]

    # resulting URL
    url = '?'.join([url_base, 
                    '&'.join(url_args)]).format(data_format=data_format.upper(),
                                                station=station.upper(), 
                                                duration=duration, 
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
        df = pd.read_csv(url, header=0, na_values=['m', '---', ' ', 'ART', 'BRT'])
        sensors.update({duration: list(df['SENSOR_TYPE'].unique())})
    return sensors


def get_station_data(station, start, end, duration=''):
    """
    General purpose function for returning a pandas DataFrame for all available
    data for CDEC `station` in the given time window, with optional `duration` argument.
    """
    return get_raw_station_csv(station, start, end, duration)


def get_raw_station_csv(station, start, end, duration='', filename=''):
    """
    Use CDEC CSV query URL to download available data.  Optional `filename` argument
    specifies custom file location for download of CSV records.
    """
    url = get_station_url(station, start, end, data_format='CSV', duration=duration)
    df = pd.read_csv(url, 
                     header=0, 
                     parse_dates=True, 
                     index_col=4, 
                     na_values=['m', '---', ' ', 'ART', 'BRT'])

    if bool(filename):
        df.to_csv(filename)

    return df


def get_raw_station_json(station, start, end, duration='', filename=''):
    """
    Use CDEC JSON query URL to download available data.  Optional `filename` argument
    specifies custom file location for download of validated JSON records.
    """

    url = get_station_url(station, start, end, data_format='JSON', duration=duration)
    response = requests.get(url, stream=True)
    result = json.loads(response.text.replace('\r\n', ', ').replace('}, ], ', '}]'))

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
        df = raw.loc[raw['SENSOR_TYPE']==sensor].loc[raw['DUR_CODE']==duration]
    elif bool(sensor):
        df = raw.loc[raw['SENSOR_TYPE']==sensor]
    else:
        raise ValueError('sensor `{}` is not valid for station `{}`'.format(sensor, station))
    df.index = pd.to_datetime(df['ACTUAL_DATE'])
    
    return df