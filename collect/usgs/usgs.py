"""
collect.usgs.usgs
============================================================
USGS National Water Information System (NWIS)
"""
# -*- coding: utf-8 -*-
import datetime as dt
from bs4 import BeautifulSoup
import dateutil.parser
import pandas as pd
import requests


def get_query_url(station_id, sensor, start_time, end_time, interval):
    """
    construct the station/sensor query URL for USGS JSON data service

    Arguments:
        station_id (int or str): the USGS station code (ex: 11446220)
        sensor (str): the timeseries sensor code, ex: '00060' (discharge)
        start_time (dt.datetime): start of timeseries window
        end_time (dt.datetime): end of timeseries window
        interval (str): the data interval, lowercase, ex: 'daily' or 'instantaneous'
    Returns:
        url (str): the query url representing the specified station and parameters
    """
    interval_code = {'instantaneous': 'i', 'daily': 'd'}[interval]

    if interval == 'instantaneous':
        format_start = start_time.isoformat()
        format_end = end_time.isoformat()

    elif interval == 'daily':
        format_start = start_time.strftime('%Y-%m-%d')
        format_end = end_time.strftime('%Y-%m-%d')

    # construct query URL
    url = '&'.join([f'https://waterservices.usgs.gov/nwis/{interval_code}v/?format=json',
                    f'sites={station_id}',
                    f'startDT={format_start}',
                    f'endDT={format_end}',
                    f'parameterCd={sensor}',
                    'siteStatus=all'])
    return url


def get_data(station_id, sensor, start_time, end_time, interval='instantaneous'):
    """
    Download timeseries data from USGS database; return as dataframe
    
    Sensor Codes
    00010 Temperature, water(Max.,Min.)
    00060 Discharge(Mean)
    00095 Specific cond at 25C(Mean,Ins.)
    80154 Suspnd sedmnt conc(Mean)
    80155 Suspnd sedmnt disch(Mean)

    Arguments:
        station_id (int or str): the USGS station code (ex: 11446220)
        sensor (str): ex '00060' (discharge)
        start_time (dt.datetime): ex dt.datetime(2016, 10, 1)
        end_time (dt.datetime): ex dt.datetime(2017, 10, 1)
        interval (str): ex 'daily'
    Returns:
        (dict): result dictionary containing key value pairs for 'data' (timeseries dataframe) and 'info' (request metadata)
    """
    # force lowercase interval
    interval = interval.lower()

    # construct query URL
    url = get_query_url(station_id, sensor, start_time, end_time, interval)

    # get gage data as json
    data = requests.get(url, verify=False).json()
    
    # process timeseries info
    series = data['value']['timeSeries'][0]['values'][0]['value']
    for entry in series:
        # entry['dateTime'] = dateutil.parser.parse(entry['dateTime'])
        entry['qualifiers'] = ','.join(entry['qualifiers'])

    frame = pd.DataFrame.from_records(series, index='dateTime')
    frame.index = pd.to_datetime(frame.index)
    frame.value = frame.value.astype(float)
    frame.rename(columns={'value': str(sensor)}, inplace=True)

    # extract site metadata from json blob
    source_info = data['value']['timeSeries'][0]['sourceInfo']
    variable_info = data['value']['timeSeries'][0]['variable']
    info = {
        'site name': source_info['siteName'],
        'site number': source_info['siteCode'][0]['value'],
        'agency': source_info['siteCode'][0]['agencyCode'],
        'network': source_info['siteCode'][0]['network'],
        'latitude': source_info['geoLocation']['geogLocation']['latitude'],
        'longitude': source_info['geoLocation']['geogLocation']['longitude'],
        'sensor': variable_info['variableCode'][0]['value'],
        'variable description': variable_info['variableDescription'],
        'units': variable_info['unit']['unitCode'],
        'interval': interval
    }

    return {'data': frame, 'info': info}


def get_usgs_data(station_id, sensor, start_time, end_time, interval='instantaneous'):
    return get_data(station_id, sensor, start_time, end_time, interval=interval)


def get_peak_streamflow(station_id):
    """
    Download annual peak timeseries data from USGS database; return as dataframe
    
    Arguments:
        station_id (int or str): the USGS station code
    Returns:
        (dict): result dictionary containing key value pairs for 'data' (peak streamflow dataframe) and 'info' (request metadata)
    """

    # construct query url
    url = '?'.join(['https://nwis.waterdata.usgs.gov/nwis/peak', 
                    'site_no={station_id}&agency_cd=USGS&format=rdb']).format(station_id=station_id)

    def leap_filter(x):
        if x.split('-', 1)[-1] == '03-00':
            x = x.replace('03-00', '02-29')
        return dt.datetime.strptime(x, '%Y-%m-%d')

    # process annual peak time series from tab-delimited table
    frame = pd.read_csv(url,
                        comment='#', 
                        parse_dates=False,
                        header=0,
                        delimiter='\t')
    frame.drop(0, axis=0, inplace=True)
    frame.index = pd.to_datetime(frame['peak_dt'].apply(leap_filter))

    # load USGS site information
    result = BeautifulSoup(requests.get(url.rstrip('rdb')).content, 'lxml')
    info = {'site number': station_id, 'site name': result.find('h2').text}
    meta = result.findAll('div', {'class': 'leftsidetext'})[0]
    for div in meta.findChildren('div', {'align': 'left'}):
        if 'county' in div.text.lower():
            info['county'] = div.text.replace('\xa0', '')
        elif 'unit code' in div.text.lower():
            info['hydrologic unit code'] = div.text.split('Code ')
        elif 'latitude' in div.text.lower():
            info['latitude'] = div.text.split(', ')[0].lstrip('Latitude').replace('\xa0', '').strip()
            info['longitude'] = div.text.split(', ')[1].lstrip('Longitude').replace('\xa0', '').split('\n')[0]
            info['datum'] = div.text.split('\n')[-1].strip().replace('\xa0', '')
        elif 'drainage' in div.text.lower():
            info['drainage area'] = div.text.split('area ')[1].replace('\xa0', ' ')
        elif 'datum' in div.text.lower():
            info['gage datum'] = div.text.split('datum ')[1].replace('\xa0', ' ').strip()

    return {'data': frame, 'info': info}
