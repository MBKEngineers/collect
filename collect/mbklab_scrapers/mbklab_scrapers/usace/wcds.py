# -*- coding: utf-8 -*-
import datetime as dt

from bs4 import BeautifulSoup
import pandas as pd
import requests


def format_float(value):
    try:
        return float(value)
    except ValueError:
        return None


def get_water_year(datetime_structure):
    """
    Returns water year of current datetime object.
    -------------------------|---------------|----------------------------
    argument                 | type          |  example
    -------------------------|---------------|----------------------------
        datetime_structure   |  dt.datetime  |  dt.datetime(2016, 10, 1)
    -------------------------|---------------|----------------------------
    """
    
    YEAR = datetime_structure.year
    if datetime_structure.month < 10:
        return YEAR
    else:
        return YEAR + 1


def get_water_year_data(reservoir, water_year, interval='d'):
    """
    Scrape water year operations data from Folsom entry on USACE-SPK's WCDS.
    -----------------|---------------|----------------------------
    argument         | type          |  example
    -----------------|---------------|----------------------------
        reservoir    |  str          |  'fol'
        water_year   |  int          |  2017
        interval     |  str          |  'd'
    -----------------|---------------|----------------------------
    """
    result = []

    # USACE-SPK Folsom page
    url = '&'.join(['http://www.spk-wc.usace.army.mil/fcgi-bin/getplottext.py?plot={reservoir}r', 
        'length=wy', 
        'wy={water_year}', 
        'interval={interval}']).format(reservoir=reservoir,
                                       water_year=water_year,
                                       interval=interval)
    
    # Download url content and parse HTML
    soup = BeautifulSoup(requests.get(url).content , 'lxml')

    # Parse Folsom reservoir page for date/time of interest
    for line in soup.find('pre').text.splitlines():
        try:
            row = line.split()
            if row[0] == '2400':
                result.append({
                    'datestring': ' '.join([row[0], row[1]]), 
                    'storage': format_float(row[6]), 
                    'release': format_float(row[2]),
                    'inflow': format_float(row[3]),
                    'usace top-con': format_float(row[4]),
                    'safca top-con': format_float(row[5]),
                    'precip-basin': format_float(row[7])
                })
        except IndexError:
            pass

    df = pd.DataFrame.from_records(result, index='datestring')

    return {'data': df, 'info': {'reservoir': reservoir, 
                                 'interval': interval, 
                                 'water year': water_year}}


def get_wcds_data(reservoir, start_time, end_time, interval='d'):
    """
    Scrape water year operations data from reservoir page on USACE-SPK's WCDS.
    -----------------|---------------|----------------------------
    argument         | type          |  example
    -----------------|---------------|----------------------------
        reservoir    |  str          |  'fol'
        start_time   |  dt.datetime  |  dt.datetime(2016, 10, 1)
        end_time     |  dt.datetime  |  dt.datetime(2017, 11, 5)
        interval     |  str          |  'd'
    -----------------|---------------|----------------------------
    """
    frames = []
    for water_year in range(get_water_year(start_time), get_water_year(end_time) + 1):
        frames.append(get_water_year_data(reservoir, water_year, interval)['data'])

    df = pd.concat(frames)
    if interval == 'd':
        df.index = pd.to_datetime(df.index, format='2400 %d%b%Y')
    else:
        df.index = pd.to_datetime(df.index, format='%H%M %d%b%Y')

    return {'data': df, 'info': {'reservoir': reservoir, 
                                 'interval': interval, 
                                 'notes': 'daily data value occurs on midnight of entry date'}}

