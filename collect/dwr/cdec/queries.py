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
from collect.utils import get_web_status


def get_station_url(station, start, end, data_format='CSV', sensors=[], duration=''):
    """ 
    Generate URL for CDEC station query for CSV- or JSON-formatted data 
    
    - if no start/end supplied, then 'H' defaults to last day of data
                                     'D' defaults to last month of data
                                     'M' defaults to last year of data
    - currently start and end are required arguments

    Arguments:
        station (str): the 3-letter CDEC station ID
        start (dt.datetime): query start date
        end (dt.datetime): query end date
        data_format (str): the CSV or JSON data format
        sensors (list): list of the numeric sensor codes
        duration (str): interval code for timeseries data (ex: 'H')
    Returns:
        url (str): the CDEC query URL
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
    url_base = 'https://cdec.water.ca.gov/dynamicapp/req/{data_format}DataServlet'
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

    Arguments:
        station (str): the 3-letter CDEC station ID
        start (dt.datetime): query start date
        end (dt.datetime): query end date
    Returns:
        sensors (list): the available sensors for the station in this window
    """
    sensors = {}
    for duration in ['E', 'H', 'D', 'M']:
        url = get_station_url(station, start, end, duration=duration)
        df = pd.read_csv(url, header=0, na_values=['m', '---', ' ', 'ART', 'BRT', -9999, -9998, -9997], usecols=[0, 1, 2, 3])
        sensors.update({duration: list(df['SENSOR_TYPE'].unique())})
    return sensors


def get_station_data(station, start, end, sensors=[], duration=''):
    """
    General purpose function for returning a pandas DataFrame for all available
    data for CDEC `station` in the given time window, with optional `duration` argument.

    Arguments:
        station (str): the 3-letter CDEC station ID
        start (dt.datetime): query start date
        end (dt.datetime): query end date
        sensors (list): list of the numeric sensor codes
        duration (str): interval code for timeseries data (ex: 'H')
    Returns:
        df (pandas.DataFrame): the queried timeseries as a DataFrame
    """
    return get_raw_station_csv(station, start, end, sensors, duration)


def get_raw_station_csv(station, start, end, sensors=[], duration='', filename=''):
    """
    Use CDEC CSV query URL to download available data.  Optional `filename` argument
    specifies custom file location for download of CSV records.

    Arguments:
        station (str): the 3-letter CDEC station ID
        start (dt.datetime): query start date
        end (dt.datetime): query end date
        sensors (list): list of the numeric sensor codes
        duration (str): interval code for timeseries data (ex: 'H')
        filename (str): optional filename for locally saving data
    Returns:
        df (pandas.DataFrame): the queried timeseries as a DataFrame
    """
    # CDEC url with query parameters
    url = get_station_url(station, start, end, data_format='CSV', sensors=sensors, duration=duration)

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
                     na_values=['m', '---', ' ', 'ART', 'BRT', -9999, -9998, -9997],
                     float_precision='high',
                     dtype=default_data_types)
    df['DATE TIME'] = df.index

    if bool(filename):
        df.to_csv(filename)

    if bool(sensors):
        return df.loc[df['SENSOR_NUMBER'].isin(sensors)]

    return df


def get_raw_station_json(station, start, end, sensors=[], duration='', filename=''):
    """
    Use CDEC JSON query URL to download available data.  Optional `filename` argument
    specifies custom file location for download of validated JSON records.

    Arguments:
        station (str): the 3-letter CDEC station ID
        start (dt.datetime): query start date
        end (dt.datetime): query end date
        sensors (list): list of the numeric sensor codes
        duration (str): interval code for timeseries data (ex: 'H')
        filename (str): optional filename for locally saving data
    Returns:
        result (str): the queried timeseries as JSON
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

    Arguments:
        station (str): the 3-letter CDEC station ID
        start (dt.datetime): query start date
        end (dt.datetime): query end date
        sensor (str): the numeric sensor code
        duration (str): interval code for timeseries data (ex: 'H')
    Returns:
        df (pandas.DataFrame): the queried timeseries for a single sensor as a DataFrame
    """
    raw = get_station_data(station, start, end, sensors=[sensor], duration=duration)
    if bool(sensor) and bool(duration):
        df = raw.loc[(raw['SENSOR_NUMBER']==sensor) & (raw['DURATION']==duration)]
    elif bool(sensor):
        df = raw.loc[raw['SENSOR_TYPE']==sensor]
    else:
        raise ValueError('sensor `{}` is not valid for station `{}`'.format(sensor, station))
    return df


def get_station_metadata(station, as_geojson=False):
    """
    get the gage meta data and datum, monitor/flood/danger stage information

    Arguments:
        station (str): the 3-letter CDEC station ID
    Returns:
        info (dict): the CDEC station metadata, stored as key, value pairs
    """

    # construct URL
    url = 'https://cdec.water.ca.gov/dynamicapp/staMeta?station_id={station}'.format(station=station)

    # request info page
    soup = BeautifulSoup(requests.get(url).content, 'lxml')

    # initialize the result dictionary
    site_info = {'title':  soup.find('h2').text, 
                 'sensors': {}, 
                 'comments': {}}
    
    # tables
    tables = soup.find_all('table')

    # extract the station geographic info table
    site_info.update(_parse_station_generic_table(tables[_get_table_index('site', tables)]))

    # extract datum info if available
    # if _get_table_index('datum', tables) is not None:
    #     site_info.update(_parse_station_generic_table(tables[_get_table_index('datum', tables)]))

    # extract available sensor metadata (interval/por)
    site_info['comments'].update(_parse_station_comments_table(tables[_get_table_index('comments', tables)]))

    # extract the station datum and measurement info table
    site_info['sensors'].update(_parse_station_sensors_table(tables[_get_table_index('sensors', tables)]))

    # add site url
    site_info.update({"CDEC URL": f"<a target=\"_blank\" href=\"{url}\">{station}</a>"})

    if soup.find('a', href=True, text='Dam Information'):
        site_info.update(get_dam_metadata(station))

    if soup.find('a', href=True, text='Reservoir Information'):
        site_info.update(get_reservoir_metadata(station))

    # export a geojson feature (as dictionary)
    if as_geojson:
        return {'type': 'Feature', 
                'geometry': {'type': 'Point', 'coordinates': [float(site_info['Longitude'].strip('°')), 
                                                              float(site_info['Latitude'].strip('°'))]},
                'properties': site_info}

    return {'info': site_info}


def get_dam_metadata(station):
    """
    get the gage meta data and datum, monitor/flood/danger stage information

    Arguments:
        station (str): the 3-letter CDEC station ID
    Returns:
        info (dict): the CDEC station metadata, stored as key, value pairs
    """
    url = 'https://cdec.water.ca.gov/dynamicapp/profile?s={station}&type=dam'.format(station=station)

    # request dam info page
    soup = BeautifulSoup(requests.get(url).content, 'lxml')

    # initialize the result dictionary
    site_info = {'title':  soup.find('h2').text}
    
    # tables
    tables = soup.find_all('table')

    site_info.update(_parse_station_generic_table(tables[-1]))

    return {'dam': site_info}


def get_reservoir_metadata(station):
    """
    get the gage meta data and datum, monitor/flood/danger stage information

    Arguments:
        station (str): the 3-letter CDEC station ID
    Returns:
        info (dict): the CDEC station metadata, stored as key, value pairs
    """
    url = 'https://cdec.water.ca.gov/dynamicapp/profile?s={station}&type=res'.format(station=station)
    
    # request dam info page
    soup = BeautifulSoup(requests.get(url).content, 'lxml')

    # initialize the result dictionary
    site_info = {'title':  soup.find('h1').text}
    
    # tables
    tables = soup.find_all('table')
    site_info.update(_parse_station_generic_table(tables[0]))
    site_info.update({'monthly_averages': _parse_station_generic_table(tables[-1])})

    return {'reservoir': site_info}


def _get_table_index(table_type, tables):
    """
    Arguments:
        table_type (str): identifier for the station metadata or comments tables, etc.
        tables (list): the tables parsed from HTML via BeautifulSoup
    Returns:
        (int): the index of table on page matching the table type
    """
    if table_type == 'site':
        return 0
    elif table_type == 'datum':
        return 1 if len(tables) > 3 else None
    elif table_type == 'sensors':
        return len(tables) - 2
    elif table_type == 'comments':
        return len(tables) - 1 
    
    return None


def _parse_station_generic_table(table):
    """
    Arguments:
        table (bs4.element.Tag): the table node parsed from HTML with BeautifulSoup
    Returns:
        result (dict): dictionary of geographic and operator info for station
    """
    result = {}
    for tr in table.find_all('tr'):
        cells = tr.find_all('td')
        for i, cell in enumerate(cells):
            if i % 2 == 0:
                key = cells[i].text.strip()
                value = cells[i+1].text.strip()
                result.update({key: value})
    return result


def _parse_station_sensors_table(table):
    """
    extract station sensor availabilty, descriptions and periods of record and return
    as a nested dictionary that can be queried by sensor, then duration (interval)

    Arguments:
        table (bs4.element.Tag): the table node parsed from HTML with BeautifulSoup
    Returns:
        result (dict): dictionary of paired comment date and content for station
    """
    result = {}

    for row in table.find_all('tr'):
        sensor_description, sensor_number, duration, plot, data_collection, data_available = [x.text.strip() for x in row.find_all('td')]
        duration = duration.strip('()')

        if sensor_number not in result:
            result[sensor_number] = {}            

        result[sensor_number].update({
            duration: {'description': sensor_description, 
                       'sensor': sensor_number, 
                       'duration': duration,
                       'collection': data_collection, 
                       'availability': data_available,
                       'years': _parse_data_available(data_available)}
        })
    
    return result


def _parse_station_comments_table(table):
    """
    extracts the dated station comments and returns as a dictionary

    Arguments:
        table (bs4.element.Tag): the table node parsed from HTML with BeautifulSoup
    Returns:
        result (dict): dictionary of paired comment date and content for station
    """
    result = {}
    for tr in table.find_all('tr'):
        cleaned = [x.text.strip() for x in tr.find_all('td')]
        if len(cleaned) > 1:
            key, value = cleaned
            result.update({key: value})
    return result


def _parse_data_available(text):
    """
    returns the list of years included in summary of data availability
    
    Arguments:
        text (str): station sensor "data available" column entry
    Returns:
        result (dict): list of years covered by data availability record
    """
    start, end = text.split(' to ')
    start = dt.datetime.strptime(start, '%m/%d/%Y')
    end = dt.datetime.strptime(end, '%m/%d/%Y') if end != 'present' else dt.date.today()
    return list(range(start.year, end.year + 1))


def get_data(station, start, end, sensor='', duration=''):
    """
    return station date for a query bounded by start and end datetimes for
    a particular sensor/duration combination

    Arguments:
        station (str): the 3-letter CDEC station ID
        start (dt.datetime): query start date
        end (dt.datetime): query end date
        sensor (int): the numeric sensor code
        duration (str): interval code for timeseries data (ex: 'H')

    Returns:
        result (dict): dictionary of pandas.DataFrame and metadata dict
    """
    df = get_sensor_frame(station, start, end, sensor, duration)
    return {'data': df, **get_station_metadata(station)}
