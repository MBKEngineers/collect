# -*- coding: utf-8 -*-
import datetime as dt
import io
import os
from bs4 import BeautifulSoup
from dateutil import parser
from dotenv import load_dotenv
import pandas as pd
from pytz import timezone
import requests
from collect.utils import clean_fixed_width_headers

UTC = timezone('UTC')
PACIFIC = timezone('America/Los_Angeles')
TODAY = dt.datetime.now().strftime('%Y%m%d')


# load credentials
load_dotenv()


def get_seasonal_trend_tabular(cnrfc_id, water_year):
    """
    adapted from data accessed in py_water_supply_reporter.py
    CNRFC Ensemble Product 7
    """

    url = '?'.join(['http://www.cnrfc.noaa.gov/ensembleProductTabular.php', 
                    'id={}&prodID=7&year={}'.format(cnrfc_id, water_year)])
   
    # retrieve from public CNRFC webpage
    result = requests.get(url).content
    result = BeautifulSoup(result, 'lxml').find('pre').text.replace('#', '')

    # in-memory file buffer
    with io.StringIO(result) as buf:

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


def get_forecast_meta_deterministic(cnrfc_id, first_ordinate=False):
    """
    Get issuance time from the deterministic inflow forecast page
    """
    
    # request page with CNRFC credentials
    url = 'https://www.cnrfc.noaa.gov/restricted/graphicalRVF_tabular.php?id={0}'.format(cnrfc_id)
    basic_auth = requests.auth.HTTPBasicAuth(os.getenv('CNRFC_USER'), os.getenv('CNRFC_PASSWORD'))
    content = requests.get(url, auth=basic_auth).content

    # parse HTML content
    soup = BeautifulSoup(content, 'lxml')
    title = soup.find_all('font', {'class': 'head'})[0].text

    for td in soup.find_all('td', {'class': 'smallhead'}):
        if 'Issuance Time' in td.text:
            issue_time = parser.parse(td.next_sibling.text)
        if 'Next Issuance' in td.text:
            next_issue_time = parser.parse(td.next_sibling.text)
        if 'Plot Type' in td.text:
            plot_type = td.text.split(':')[1].strip()

    if first_ordinate:
        data = soup.find('pre').text.split('\n')
        forecast_start = parser.parse(data[data.index(u'# FORECAST')+4].strip()[:25])
        return issue_time, next_issue_time, title, plot_type, forecast_start
    
    return issue_time, next_issue_time, title, plot_type


def get_deterministic_forecast(cnrfc_id, truncate_historical=False):
    """
    Adapted from SAFCA portal project
    ---
    reads the url and returns a pandas dataframe from a file or the cnrfc url
    cnrfc_id:  CNRFC station id (5 letter id) (e.g. FOLC1)
    convert CSV data to DataFrame, separating historical from forecast inflow series
    """

    # get forecast file from csv url
    url = 'https://www.cnrfc.noaa.gov/restricted/graphicalRVF_csv.php?id={0}'.format(cnrfc_id)
    basic_auth = requests.auth.HTTPBasicAuth(os.getenv('CNRFC_USER'), os.getenv('CNRFC_PASSWORD'))
    content = requests.get(url, auth=basic_auth).content
    
    # read historical and forecast series from CSV
    with io.BytesIO(content) as csvdata:
        df = pd.read_csv(csvdata, 
                         header=0, 
                         parse_dates=[0],
                         index_col=0,
                         float_precision='high',
                         dtype={'Date/Time (Pacific Time)': str, 
                                'Flow (CFS)': float, 
                                'Trend': str})
        
    # add timezone info
    df.index = [PACIFIC.localize(x) for x in df.index]
    
    # Trend value is null for first historical and first forecast entry; select forecast entry
    first_ordinate = df.where(df['Trend'].isnull()).dropna(subset=['Flow (CFS)']).last_valid_index()

    # deterministic forecast inflow series
    df['forecast'] = df.loc[(df.index >= first_ordinate), 'Flow (CFS)']

    # optional limit for start of historical data (2 days before start of forecast)
    if truncate_historical:
        start = first_ordinate - dt.timedelta(hours=49)
        mask = (df.index > start)
    else:
        mask = True

    # historical inflow series
    df['historical'] = df.loc[(df['forecast'].isnull()) & mask]['Flow (CFS)']

    # forecast metadata
    info = {'url': url,
            'type': 'Deterministic Forecast',
            'first ordinate': first_ordinate.strftime('%Y-%m-%d %H:%M'),
            'units': 'cfs',
            'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M')}

    # additional issuance, plot-type information
    # get issue time of most recent hourly inflow forecast
    # time_issued, next_issue_time, title, plot_type = get_forecast_meta_deterministic(cnrfc_id)
    # info.update({'issuance time': time_issued.strftime('%Y-%m-%d %H:%M'),
    #              'next forecast': next_issue_time.strftime('%Y-%m-%d %H:%M'),
    #              'title': title,
    #              'plot_type': plot_type})

    return {'data': df, 'info': info}


if __name__ == '__main__':

    from pprint import pprint
    RESERVOIRS = {'Folsom': 'FOLC1',
                  'New Bullards Bar': 'NBBC1',
                  'Oroville': 'ORDC1',
                  'Pine Flat': 'PNFC1',
                  'Shasta': 'SHDC1'}

    pprint(get_deterministic_forecast('SHDC1', truncate_historical=False)['info'])

    # print(get_seasonal_trend_tabular('SHDC1', 2018)['data'])