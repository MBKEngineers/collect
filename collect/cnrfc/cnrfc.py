# -*- coding: utf-8 -*-
import datetime as dt
import io
import os
from bs4 import BeautifulSoup
import pandas as pd
import requests
from collect.utils import clean_fixed_width_headers


def get_seasonal_trend_tabular(cnrfc_id, water_year):
    """
    adapted from data accessed in py_water_supply_reporter.py
    """

    url = '?'.join(['http://www.cnrfc.noaa.gov/ensembleProductTabular.php', 
                    'id={}&prodID=7&year={}'.format(cnrfc_id, water_year)])
   
    # retrieve from public CNRFC webpage
    result = requests.get(url).content
    result = BeautifulSoup(result, 'lxml').find('pre').text.replace('#', '')

    # in-memory file buffer
    buf = io.StringIO()
    buf.write(result)
    buf.seek(0)

    # parse fixed-width text-formatted table
    df = pd.read_fwf(buf, 
                     header=[1, 2, 3, 4, 5], 
                     skiprows=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 16], 
                     na_values=['<i>Missing</i>', 'Missing'])
    
    # clean columns
    df.columns = clean_fixed_width_headers(df.columns)

    # clean missing data rows
    df.dropna(subset=['Date (mm/dd/YYYY)'], inplace=True)
    df.drop(df.last_valid_index(), axis=0, inplace=True)

    # parse dates
    df.index = pd.to_datetime(df['Date (mm/dd/YYYY)'])
    df.index.name = 'Date'

    # close buffer
    buf.close()

    # parse summary from pre-table notes
    notes = result.splitlines()[:10]
    summary = {}
    for line in notes[2:]:
        if bool(line.strip()):
            k, v = line.strip().split(': ')
            summary.update({k: v.strip()})
    
    return {'data': df, 'info': {'url': url,
                                 'type': 'Seasonal Trend Tabular (Apr-Jul)',
                                 'title': notes[0],
                                 'summary': summary,
                                 'units': 'TAF',
                                 'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M')}}


if __name__ == '__main__':

    RESERVOIRS = {'Folsom': 'FOLC1',
                  'New Bullards Bar': 'NBBC1',
                  'Oroville': 'ORDC1',
                  'Pine Flat': 'PNFC1',
                  'Shasta': 'SHDC1'}

    print(get_seasonal_trend_tabular('SHDC1', 2018)['data'])