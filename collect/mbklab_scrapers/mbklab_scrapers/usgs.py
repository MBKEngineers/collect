# -*- coding: utf-8 -*-
import requests
import dateutil.parser
import pandas as pd


def get_usgs_data(station_id, sensor, start_time, end_time, interval='instantaneous'):
    """
    Download timeseries data from USGS database; return as dataframe
    ---------------------|---------------|----------------------------
    argument             | type          |  example
    ---------------------|---------------|----------------------------
        station_id       |  int or str   |  11446220
        sensor           |  str          |  '00060' (discharge)
        start_time       |  dt.datetime  |  dt.datetime(2016, 10, 1)
        end_time         |  dt.datetime  |  dt.datetime(2017, 10, 1)
        interval         |  str          |  'daily'
    ---------------------|---------------|----------------------------

    Sensor Codes
    ---------------------------------------
    00010 Temperature, water(Max.,Min.)
    00060 Discharge(Mean)
    00095 Specific cond at 25C(Mean,Ins.)
    80154 Suspnd sedmnt conc(Mean)
    80155 Suspnd sedmnt disch(Mean)
    """
    if interval == 'instantaneous':
        format_start = start_time.isoformat()
        format_end = end_time.isoformat()

    elif interval == 'daily':
        format_start = start_time.strftime('%Y-%m-%d')
        format_end = end_time.strftime('%Y-%m-%d')

    # construct query URL
    url = '&'.join([
        'https://waterservices.usgs.gov/nwis/{interval}v/?format=json',
        'sites={station_id}',
        'startDT={start_time}',
        'endDT={end_time}',
        'parameterCd={sensor}&siteStatus=all'
    ]).format(
        interval={'instantaneous':'i', 'daily':'d'}[interval],
        station_id=station_id, 
        start_time=format_start, 
        end_time=format_end,
        sensor=sensor
    )

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
