# -*- coding: utf-8 -*-
import datetime as dt
from io import StringIO
import socket
import zipfile
from bs4 import BeautifulSoup
from dateutil import parser
import pandas as pd
from pytz import timezone
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import urllib3


UTC = timezone('UTC')
PACIFIC = timezone('America/Los_Angeles')
TODAY = dt.datetime.now().strftime('%Y%m%d')


"""
hh can be 12, 18, 00, 06


Deterministic Inflow/Release Forecasts:
---------------------------------------
https://www.cnrfc.noaa.gov/restricted/reservoir.php?id=FOLC1
https://www.cnrfc.noaa.gov/restricted/graphicalRVF_csv.php?id=FOLC1
https://www.cnrfc.noaa.gov/restricted/graphicalRVF_tabular.php?id=FOLC1
https://www.cnrfc.noaa.gov/restricted/graphicalRelease_tabular.php?id=ORDC1

https://www.cnrfc.noaa.gov/deterministicHourlyProductCSV.php
http://www.cnrfc.noaa.gov/csv/YYYYmmddhh_american_csv_export.zip


Ensemble Traces:
---------------------------------------
http://www.cnrfc.noaa.gov/ensembleProduct.php?id=FOLC1&prodID=4
http://www.cnrfc.noaa.gov/csv/FOLC1_hefs_csv_hourly.csv    
https://www.cnrfc.noaa.gov/ensembleHourlyProductCSV.php
https://www.cnrfc.noaa.gov/ensembleProductCSV.php

http://www.cnrfc.noaa.gov/csv/YYYYmmddhh_american_hefs_csv_daily.zip
http://www.cnrfc.noaa.gov/csv/YYYYmmddhh_american_hefs_csv_hourly.zip

"""

# disable warnings in crontab logs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_station_hourly_ensemble(station_id, acre_feet=False, pdt_convert=False, as_pdt=False):
    """
    possibly CNRFC is labeling GMT when it's actually alread in PDT/PST??? - 13Feb2019
    date_string = time_issued.strftime('%Y%m%d_%H%M')
    """
    
    # get issue time of most recent hourly inflow forecast
    time_issued = cnrfc_hourly_forecast_issue_time()
    
    # forecast data url
    url = 'https://www.cnrfc.noaa.gov/csv/{0}_hefs_csv_hourly.csv'.format(station_id)

    # fetch hourly ensemble forecast data
    basic_auth = requests.auth.HTTPBasicAuth(settings.CNRFC_USER, settings.CNRFC_PASSWORD)
    session = requests.Session()
    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    content = session.get(url, auth=basic_auth, verify=False).text
    csvdata = StringIO()
    csvdata.write(content)
    csvdata.seek(0)

    df = pd.read_csv(csvdata, 
                     header=0, 
                     skiprows=[1], 
                     parse_dates=[0], 
                     index_col=0, 
                     float_precision='high',
                     dtype={'GMT': str, station_id: float})

    # rename columns for ensemble member IDs starting at 1950
    df.columns = [str(x) for x in range(1950, 1950 + len(df.columns))]
    
    # convert kcfs/day to cfs/day
    df = df * 1000.0

    if acre_feet:
        df = df * ( 3600 / 43560.0 )

    if pdt_convert:
        df.index = df.index.tz_localize('UTC').tz_convert('America/Los_Angeles')
        df.index.name = 'America/Los_Angeles'
    
    elif as_pdt:
        df.index = [PACIFIC.localize(x) for x in df.index]
        df.index.name = 'America/Los_Angeles'

    return df


def cnrfc_hourly_forecast_issue_time(deterministic=True, first_ordinate=False, cnrfc_id='FOLC1'):
    """
    Get issuance time from the deterministic inflow forecast page, as other sources
    not available for FOLC1 in particular.    
    """
    if deterministic:
        url = 'https://www.cnrfc.noaa.gov/restricted/graphicalRVF_tabular.php?id={0}'.format(cnrfc_id)
    else:
        return get_watershed_ensemble_issue_time('hourly')
    
    # request page with CNRFC credentials
    basic_auth = requests.auth.HTTPBasicAuth(settings.CNRFC_USER, settings.CNRFC_PASSWORD)
    content = requests.get(url, auth=basic_auth).content

    # parse HTML content
    soup = BeautifulSoup(content, 'lxml')
    for td in soup.find_all('td', {'class': 'smallhead'}):
        if 'Issuance Time' in td.text:
            issue_time = parser.parse(td.next_sibling.text)

    if first_ordinate:
        data = soup.find('pre').text.split('\n')
        forecast_start = parser.parse(data[data.index(u'# FORECAST')+4].strip()[:25])
        return issue_time, forecast_start
    
    return issue_time


def download_cnrfc_deterministic_forecast(station_id, truncate_historical=False):
    """
    reads the url and returns a pandas dataframe from a file or the cnrfc url
    station_id:  CNRFC station id (5 letter id) (e.g. FOLC1)
    convert CSV data to DataFrame, separating historical from forecast inflow series

    """
    
    # get issue time of most recent hourly inflow forecast
    time_issued, first_ordinate = cnrfc_hourly_forecast_issue_time(deterministic=True, first_ordinate=True)   
    date_string = time_issued.strftime('%Y%m%d_%H%M')

    # forecast information
    units = 'cfs'
    
    # get forecast file from csv url
    url = 'https://www.cnrfc.noaa.gov/restricted/graphicalRVF_csv.php?id={0}'.format(station_id)
    basic_auth = requests.auth.HTTPBasicAuth(os.getenv('CNRFC_USER'), os.getenv('CNRFC_PASSWORD'))
    content = requests.get(url, auth=basic_auth).content
    csvdata = StringIO()
    csvdata.write(content)
    csvdata.seek(0)

    df = pd.read_csv(csvdata, 
                     header=0, 
                     parse_dates=[0],
                     index_col=0,
                     float_precision='high',
                     dtype={'Date/Time (Pacific Time)': str, 
                            'Flow (CFS)': float, 
                            'Trend': str})
    df.index = [PACIFIC.localize(x) for x in df.index]
    df = df[[u'Flow (CFS)']]
    
    # deterministic forecast inflow series
    df['forecast'] = df.loc[(df.index >= self.first_forecast_ordinate)]

    # optional limit for start of historical data (2 days before start of forecast)
    if truncate_historical:
        start = self.first_forecast_ordinate - dt.timedelta(hours=49)
        mask = (df.index > start)
    else:
        mask = True

    # historical inflow series
    df['historical'] = df.loc[(df['forecast'].isnull()) & mask]['Flow (CFS)']

    csvdata.close()
    return time_issued, df


def get_ensemble_first_forecast_ordinate(url):
    """
    return the first date of the forecast (GMT) as datetime object
    """
    df = pd.read_csv(url, 
                     nrows=1, 
                     header=0, 
                     skiprows=[1], 
                     parse_dates=[0], 
                     index_col=0, 
                     float_precision='high',
                     dtype={'GMT': str, 'FOLC1': float})

    return df.index.tolist()[0].to_pydatetime()


def get_watershed_ensemble_issue_time(duration, date_string=None):
    """
    get "last modified" date/time stamp from CNRFC watershed ensemble product table
    """
    if duration[0].upper() == 'D':
        url = 'https://www.cnrfc.noaa.gov/ensembleProductCSV.php'
        duration = 'daily'
    elif duration[0].upper() == 'H':
        url = 'https://www.cnrfc.noaa.gov/ensembleHourlyProductCSV.php'
        duration = 'hourly'
    date_string = default_date_string(date_string) 
    content = requests.get(url, verify=False).content
    soup = BeautifulSoup(content, 'lxml')
    for td in soup.find_all('td', {'class': 'table-listing-content'}):
        if '{0}_american_hefs_csv_{1}.zip'.format(date_string, duration) in td.text:
            issue_time = parser.parse(td.next_sibling.text).astimezone(PACIFIC)
            return issue_time


def get_watershed_ensemble_daily(date_string=None):
    """ 
    download seasonal outlook for the American River Watershed as zipped file, unzip...
    """

    date_string = default_date_string(date_string)

    if date_string[-2:] != '12':
        raise ValueError('date_string must be of form %Y%m%d12.')

    # data source
    url = 'http://www.cnrfc.noaa.gov/csv/{0}_american_hefs_csv_daily.zip'.format(date_string)
    filename = url.split('/')[-1].replace('.zip', '.csv')

    session = requests.Session()
    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[ 500, 502, 503, 504 ])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    content = session.get(url, verify=False).content

    # store content in memory and parse zipped file
    zipdata = StringIO()
    zipdata.write(content)
    zip_ref = zipfile.ZipFile(zipdata)

    # extract CSV from zip object
    csvdata = StringIO()       
    csvdata.write(zip_ref.read(filename.replace('.zip', '.csv')))
    csvdata.seek(0)
    zip_ref.close()

    # get date/time stamp from ensemble download page
    try:
        time_issued = get_watershed_ensemble_issue_time('daily', date_string)
    except:
        time_issued = UTC.localize(dt.datetime.strptime(date_string, '%Y%m%d12')).strftime('%Y-%m-%d 12:00')

    daily_ensemble_csv = File(file=csvdata, name=filename)
    ensemble = True
    deterministic = False
    units = 'kcfs'
    watershed = 'american'

    # clean up
    zipdata.close()
    csvdata.close()


def default_date_string(date_string):
    if date_string is None:
        now = dt.datetime.today()
        date_string = now.strftime('%Y%m%d{0}'.format(6 * round(now.hour//6)))
    return date_string

# feeds manager
# check_url_exists
# extract upstream reservoir hourly forecasts and deterministic forecasts from watershed hourly forecast zipfiles
