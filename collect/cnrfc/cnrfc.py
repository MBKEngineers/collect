"""
collect.cnrfc.cnrfc
============================================================
access CNRFC forecasts
"""
# -*- coding: utf-8 -*-
import datetime as dt
import io
import math
import os
import zipfile
from bs4 import BeautifulSoup
from dateutil import parser
from dotenv import load_dotenv
import pandas as pd
import pytz
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from collect.cnrfc.gages import *
from collect.utils.utils import clean_fixed_width_headers, get_web_status

UTC = pytz.timezone('UTC')
PACIFIC = pytz.timezone('America/Los_Angeles')
TODAY = dt.datetime.now().strftime('%Y%m%d')


# load credentials
load_dotenv()

# disable warnings in crontab logs
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_seasonal_trend_tabular(cnrfc_id, water_year):
    """
    CNRFC Ensemble Product 7, includes Apr-Jul Forecast 90%, 75%, 50%, 25%, and 10% Exceedance, NWS Apr-Jul Forecast,  
    Raw Obs Apr-Jul To Date, Raw Avg Apr-Jul To Date, Raw Daily Observation
    adapted from data accessed in py_water_supply_reporter.py
    example url: https://www.cnrfc.noaa.gov/ensembleProductTabular.php?id=HLEC1&prodID=7&year=2013
    Arguments:
        cnrfc_id (str): forecast point (such as FOLC1)
        water_year (str/int): water year for forecast
    Returns:
        (dict): data and info
    """

    url = get_ensemble_product_url(product_id=7, cnrfc_id=cnrfc_id, data_format='Tabular')
    url += '&year={0}'.format(water_year)

    assert int(water_year) >= 2011, "Ensemble Forecast Product 7 not available before 2011"
   
    # retrieve from public CNRFC webpage
    result = BeautifulSoup(_get_cnrfc_restricted_content(url), 'lxml').find('pre').text.replace('#', '')

    # in-memory file buffer
    with io.StringIO(result) as buf:

        # parse fixed-width text-formatted table
        df = pd.read_fwf(buf, 
                         header=[0, 1, 2, 3, 4], 
                         skiprows=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 16], 
                         na_values=['<i>Missing</i>', 'Missing'])

    # clean columns and fix spelling in source
    df.columns = clean_fixed_width_headers(df.columns)
    df.rename({x: x.replace('Foreacst', 'Forecast').replace('Foreacast', 'Forecast') 
               for x in df.columns}, axis=1, inplace=True)

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


def get_water_year_trend_tabular(cnrfc_id, water_year):
    """
    CNRFC Ensemble Product 9, which includes WY Forecast 90% Exceedance, 75% Exceedance, 50% Exceedance, 25% Exceedance, 
    10% Exceedance, Raw WY To Date Observation, Raw WY To Date Average, Raw Daily Observation
    #example url: https://www.cnrfc.noaa.gov/ensembleProductTabular.php?id=FOLC1&prodID=9&year=2022#
    Arguments:
        cnrfc_id (str): forecast point (such as FOLC1)
        water_year (str/int): water year for forecast
    Returns:
        (dict): data and info
    """

    url = get_ensemble_product_url(product_id=9, cnrfc_id=cnrfc_id, data_format='Tabular')
    url += '&year={0}'.format(water_year)

    assert int(water_year) >= 2013, "Ensemble Forecast Product 9 not available before 2013"
   
    # retrieve from public CNRFC webpage
    result = BeautifulSoup(_get_cnrfc_restricted_content(url), 'lxml').find('pre').text.replace('#', '')

    # in-memory file buffer
    with io.StringIO(result) as buf:

        # parse fixed-width text-formatted table
        df = pd.read_fwf(buf, 
                         header=[0, 1, 2, 3], 
                         skiprows=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 17], 
                         na_values=['<i>Missing</i>', 'Missing'])

    # clean columns and fix spelling in source
    df.columns = clean_fixed_width_headers(df.columns)
    df.rename({x: x.replace('Foreacst', 'Forecast').replace('Foreacast', 'Forecast') 
               for x in df.columns}, axis=1, inplace=True)

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
                                 'type': 'Water Year Trend Tabular',
                                 'title': notes[0],
                                 'summary': summary,
                                 'units': 'TAF',
                                 'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M')}}


def get_deterministic_forecast(cnrfc_id, truncate_historical=False, release=False):
    """
    Adapted from SAFCA portal project
    ---
    reads the url and returns a pandas dataframe from a file or the CNRFC url
    cnrfc_id:  CNRFC station id (5 letter id) (e.g. FOLC1)
    convert CSV data to DataFrame, separating historical from forecast inflow series

    Note: as of March 2022, deterministic forecasts retrieved with the graphicalRVF or
          graphicalRelease URLs return CSVs of 3 different formats with headers that 
          may also include stage information

    Arguments:
        cnrfc_id (str): the forecast location ID
        truncate_historical (bool): flag for whether to trim historical timeseries from record
        release (bool): flag for whether to query deterministic release forecast
    Returns:
        (dict): dictionary result with dataframe containing forecast data and additional metadata
    """
    # forecast type
    forecast_type = 'Release' if release else 'RVF'
    flow_prefix = 'Release ' if release else ''

    # default deterministic URL and index name
    url = 'https://www.cnrfc.noaa.gov/graphical{0}_csv.php?id={1}'.format(forecast_type, cnrfc_id)
    date_column_header = 'Valid Date/Time (Pacific)'
    specified_dtypes = {date_column_header: str, 
                        'Stage (Feet)': float,
                        f'{flow_prefix}Flow (CFS)': float, 
                        'Trend': str,
                        'Issuance Date/Time (Pacific)': str,
                        'Threshold Exceedance Status': str,
                        'Observed/Forecast': str}

    # use restricted site url for certain forecast locations
    if cnrfc_id in RESTRICTED:
        url = 'https://www.cnrfc.noaa.gov/restricted/graphical{0}_csv.php?id={1}'.format(forecast_type, cnrfc_id)
        date_column_header = 'Date/Time (Pacific Time)'
        specified_dtypes = {date_column_header: str, 
                            f'{flow_prefix}Flow (CFS)': float, 
                            'Trend': str}

    # get forecast file from csv url
    csvdata = _get_forecast_csv(url)

    # read historical and forecast series from CSV
    df = pd.read_csv(csvdata, 
                     header=0, 
                     parse_dates=True,
                     float_precision='high',
                     dtype=specified_dtypes)

    df.set_index(date_column_header, inplace=True)
    df.index = pd.to_datetime(df.index)

    # add timezone info
    df.index.name = 'PDT/PST'

    # Trend value is null for first historical and first forecast entry; select forecast entry
    first_ordinate = df.where(df['Trend'].isnull()).dropna(subset=[f'{flow_prefix}Flow (CFS)']).last_valid_index()

    # deterministic forecast inflow series
    df['forecast'] = df.loc[(df.index >= first_ordinate), f'{flow_prefix}Flow (CFS)']

    # optional limit for start of historical data (2 days before start of forecast)
    if truncate_historical:
        start = first_ordinate - dt.timedelta(hours=49)
        mask = (df.index > start)
    else:
        mask = True

    # historical inflow series
    df['historical'] = df.loc[(df['forecast'].isnull()) & mask][f'{flow_prefix}Flow (CFS)']

    # additional issuance, plot-type information
    time_issued, next_issue_time, title, plot_type = get_forecast_meta_deterministic(cnrfc_id)

    return {'data': df, 'info': {'url': url,
                                 'type': f'Deterministic {flow_prefix}Forecast',
                                 'title': title,
                                 'plot_type': plot_type,                                 
                                 'first_ordinate': first_ordinate.strftime('%Y-%m-%d %H:%M'),
                                 'issue_time': time_issued.strftime('%Y-%m-%d %H:%M'),
                                 'next_issue': next_issue_time.strftime('%Y-%m-%d %H:%M'),
                                 'units': 'cfs',
                                 'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M')}}


def get_deterministic_forecast_watershed(watershed, date_string, acre_feet=False, pdt_convert=False, as_pdt=False, cnrfc_id=None):
    """
    download the deterministic forecasts for an entire watershed, as linked on
    https://www.cnrfc.noaa.gov/deterministicHourlyProductCSV.php

    Arguments:
        watershed (str): the string identifier for the watershed
        date_string (str): date as a string in format YYYYMMDDHH
        acre_feet (bool): flag to convert flow forecasts to volumes in acre-feet
        pdt_convert (bool): flag to convert to Pacific timezone
        as_pdt (bool): localize the data in Pacific timezone
        cnrfc_id (str): optional single forecast location for filtering
    Returns:
        (dict): resulting dictionary with data key mapping to dataframe and info containing query metadata
    """
    units = 'kcfs'

    # store original date_string
    _date_string = date_string

    # forecast datestamp prefix
    date_string = _default_date_string(date_string)

    # data source
    url = 'https://www.cnrfc.noaa.gov/csv/{0}_{1}_csv_export.zip'.format(date_string, watershed)

    # extract CSV from zip object
    if get_web_status(url):
        try:
            csvdata = _get_forecast_csv(url)
        except zipfile.BadZipFile:
            print(f'ERROR: forecast for {date_string} has not yet been issued.')
            raise zipfile.BadZipFile
    
    # raise error if user supplied an actual date string but that forecast doesn't exist
    elif _date_string is not None:
        print(f'ERROR: forecast for {date_string} has not yet been issued.')
        raise zipfile.BadZipFile

    # try previous forecast until a valid file is found
    else:
        stamp = dt.datetime.strptime(date_string, '%Y%m%d%H')
        while not get_web_status(url):            
            stamp -= dt.timedelta(hours=6)
            url = 'https://www.cnrfc.noaa.gov/csv/{0:%Y%m%d%H}_{1}_csv_export.zip'.format(stamp, watershed)
        date_string = stamp.strftime('%Y%m%d%H')
        csvdata = _get_forecast_csv(url)

    # parse forecast data from CSV
    df = pd.read_csv(csvdata, 
                     header=0, 
                     skiprows=[1,], 
                     parse_dates=True, 
                     index_col=0,
                     float_precision='high',
                     dtype={'GMT': str})

    # filter watershed for single forecast point ensemble, if provided
    if cnrfc_id is not None:
        df = df.filter(regex=r'^{0}((\.\d+)?)$'.format(cnrfc_id))

    # convert kcfs to cfs; optional timezone conversions and optional conversion to acre-feet
    df, units = _apply_conversions(df, 'hourly', acre_feet, pdt_convert, as_pdt)

    # clean up
    csvdata.close()

    # forecast issue time
    time_issued = get_watershed_forecast_issue_time('hourly', watershed, date_string, deterministic=True)

    return {'data': df, 'info': {'url': url, 
                                 'type': 'Deterministic Forecast', 
                                 'issue_time': time_issued.strftime('%Y-%m-%d %H:%M') if time_issued is not None else time_issued,
                                 'watershed': watershed, 
                                 'units': units,
                                 'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M')}}


def get_forecast_meta_deterministic(cnrfc_id, first_ordinate=False, release=False):
    """
    Get issuance time from the deterministic inflow forecast page
    
    Arguments:
        cnrfc_id (str): the 5-character CNRFC forecast location code
        first_ordinate (bool): flag for whether to extract first forecast timestep
        release (bool): flag for whether to query deterministic release forecast
    Returns:
        (tuple): tuple of forecast issuance time, next issuance time (as datetimes) and plot_type (None)
    """
    # defaults
    issue_time, next_issue_time, plot_type = None, None, None

    # request page with CNRFC credentials and parse HTML content
    url = 'https://www.cnrfc.noaa.gov/{1}graphical{2}_tabular.php?id={0}'.format(cnrfc_id, 
                                                                        'restricted/' if cnrfc_id in RESTRICTED else '',
                                                                        'Release' if release else 'RVF')
    soup = BeautifulSoup(_get_cnrfc_restricted_content(url), 'lxml')
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


def get_ensemble_forecast(cnrfc_id, duration, acre_feet=False, pdt_convert=False, as_pdt=False):
    """
    ported from SAFCA Portal project

    The hourly ensemble traces (product 4) are available at this URL pattern for certain forecast points
    https://www.cnrfc.noaa.gov/ensembleProduct.php?id=XXXC1&prodID=4

    The csv is directly available at 
    https://www.cnrfc.noaa.gov/csv/XXXC1_hefs_csv_XXXXX.csv

    Arguments:
        cnrfc_id (str): the 5-character CNRFC forecast location code
        duration (str): forecast data timestep (hourly or daily)
        acre_feet (bool): flag to convert flows to volumes
        pdf_convert (bool): flag to convert from UTC/GMT to Pacific timezone
        as_pdt (bool): flag to parse datetimes assuming Pacific timezone (no conversion from UTC)
    Returns:
        (dict): dictionary with data (dataframe) entry and info metadata dict
    """

    # default ensemble forecast units    
    units = 'kcfs'

    # validate duration
    duration = _validate_duration(duration)
    
    # get issue time of most recent hourly inflow forecast (no support for daily yet)
    date_string = _default_date_string(None)
    time_issued = get_watershed_forecast_issue_time(duration, get_watershed(cnrfc_id), date_string)

    # forecast data url
    url = 'https://www.cnrfc.noaa.gov/csv/{0}_hefs_csv_{1}.csv'.format(cnrfc_id, duration)
   
    # read forecast ensemble series from CSV
    csvdata = _get_forecast_csv(url)
    df = pd.read_csv(csvdata, 
                     header=0, 
                     skiprows=[1], 
                     parse_dates=True, 
                     index_col=0, 
                     float_precision='high', 
                     dtype={'GMT': str, cnrfc_id: float})

    # rename columns for ensemble member IDs starting at 1
    df.columns = [str(x) for x in range(1, 1 + len(df.columns))]
    
    # convert kcfs to cfs; optional timezone conversions and optional conversion to acre-feet
    df, units = _apply_conversions(df, duration, acre_feet, pdt_convert, as_pdt)

    return {'data': df, 'info': {'url': url, 
                                 'watershed': get_watershed(cnrfc_id), 
                                 'type': '{0} Ensemble Forecast'.format(duration.title()),
                                 'issue_time': time_issued.strftime('%Y-%m-%d %H:%M') if time_issued is not None else time_issued,
                                 'first_ordinate': get_ensemble_first_forecast_ordinate(df=df).strftime('%Y-%m-%d %H:%M'),
                                 'units': units, 
                                 'duration': duration,
                                 'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M')}}


def get_ensemble_forecast_watershed(watershed, duration, date_string, acre_feet=False, pdt_convert=False, as_pdt=False, cnrfc_id=None):
    """
    from: get_watershed_ensemble_issue_time
          get_watershed_ensemble_daily

    download seasonal outlook for the watershed as zipped file, unzip...

    Arguments:
        watershed (str): the forecast group identifier
        duration (str): forecast data timestep (hourly or daily)
        date_string (str): the forecast issuance date as a YYYYMMDDHH formatted string
        acre_feet (bool): flag to convert flows to volumes
        pdf_convert (bool): flag to convert from UTC/GMT to Pacific timezone
        as_pdt (bool): flag to parse datetimes assuming Pacific timezone (no conversion from UTC)
        cnrfc_id (str): the 5-character CNRFC forecast location code
    Returns:
        (dict): dictionary with data (dataframe) entry and info metadata dict
    """
    units = 'kcfs'

    duration = _validate_duration(duration)

    # store original date_string
    _date_string = date_string

    # forecast datestamp prefix
    date_string = _default_date_string(date_string)

    # data source
    url = 'https://www.cnrfc.noaa.gov/csv/{0}_{1}_hefs_csv_{2}.zip'.format(date_string, watershed, duration)

    # extract CSV from zip object
    if get_web_status(url):
        try:
            csvdata = _get_forecast_csv(url)
        except zipfile.BadZipFile:
            print(f'ERROR: forecast for {date_string} has not yet been issued.')
            raise zipfile.BadZipFile
    
    # raise error if user supplied an actual date string but that forecast doesn't exist
    elif _date_string is not None:
        print(f'ERROR: forecast for {date_string} has not yet been issued.')
        raise zipfile.BadZipFile

    # try previous forecast until a valid file is found
    else:
        stamp = dt.datetime.strptime(date_string, '%Y%m%d%H')
        while not get_web_status(url):            
            stamp -= dt.timedelta(hours=6)
            url = 'https://www.cnrfc.noaa.gov/csv/{0:%Y%m%d%H}_{1}_hefs_csv_{2}.zip'.format(stamp, watershed, duration)
        date_string = stamp.strftime('%Y%m%d%H')
        csvdata = _get_forecast_csv(url)

    # parse forecast data from CSV
    df = pd.read_csv(csvdata, 
                     header=0, 
                     skiprows=[1,], 
                     parse_dates=True, 
                     index_col=0,
                     float_precision='high',
                     dtype={'GMT': str})

    # filter watershed for single forecast point ensemble, if provided
    if cnrfc_id is not None:
        df = df.filter(regex=r'^{0}((\.\d+)?)$'.format(cnrfc_id))
    
    # convert kcfs to cfs; optional timezone conversions and optional conversion to acre-feet
    df, units = _apply_conversions(df, duration, acre_feet, pdt_convert, as_pdt)

    # get date/time stamp from ensemble download page
    time_issued = get_watershed_forecast_issue_time(duration, watershed, date_string)
    
    return {'data': df, 'info': {'url': url, 
                                 'watershed': watershed, 
                                 'issue_time': time_issued.strftime('%Y-%m-%d %H:%M') if time_issued is not None else time_issued,
                                 'first_ordinate': get_ensemble_first_forecast_ordinate(df=df).strftime('%Y-%m-%d %H:%M'),
                                 'units': units, 
                                 'duration': duration,
                                 'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M')}}


def get_watershed_forecast_file(watershed, date_string, forecast_type, duration=None, path=None):
    """

    download short range ensemble, deterministic forecast, and seasonal outlook for the 
    watershed as zipped file, unzip, save as csv to path

    Arguments:
        watershed (str): the forecast group identifier
        date_string (str): the forecast issuance date as a YYYYMMDDHH formatted string
        forecast_type (str): one of deterministic or ensemble
        duration (str): forecast data timestep (hourly or daily)
        path (str): file name including file path/s3 key
    Returns:
        (path): file name including file path/s3 key
    """
    # store original date_string
    _date_string = date_string

    # forecast datestamp prefix
    date_string = _default_date_string(date_string)

    # define URL
    url_end = ''

    if forecast_type == 'deterministic':
        url_end = '{0}_{1}_csv_export.zip'.format(date_string, watershed)
    elif forecast_type == 'ensemble':
        duration = _validate_duration(duration)
        url_end = '{0}_{1}_hefs_csv_{2}.zip'.format(date_string, watershed, duration)
    
    url = 'https://www.cnrfc.noaa.gov/csv/' + url_end

    # extract CSV from zip object
    if get_web_status(url):
        try:
            csvdata = _get_forecast_csv(url)
        except zipfile.BadZipFile:
            print(f'ERROR: forecast for {date_string} has not yet been issued.')
            raise zipfile.BadZipFile
    
    # raise error if user supplied an actual date string but that forecast doesn't exist
    elif _date_string is not None:
        print(f'ERROR: forecast for {date_string} has not yet been issued.')
        raise zipfile.BadZipFile

    # set path for case where path set to None
    if path is None:
        path = '{}/{}'.format(os.getcwd(), 
                                url.split('/')[-1].replace('.zip','.csv'))

    # write csvdata to specified path
    path = path.replace('/', os.sep)
    with open(path, 'wb') as f:
        f.write(csvdata.read())

    return path

def get_watershed_forecast_issue_time(duration, watershed, date_string=None, deterministic=False):
    """
    get "last modified" date/time stamp from CNRFC watershed ensemble product table
    """
    duration = _validate_duration(duration)

    if duration == 'daily':
        #" only on the 12"
        date_string = date_string[:-2] + '12'
        url = 'https://www.cnrfc.noaa.gov/ensembleProductCSV.php'
        file_name = '{0}_{1}_hefs_csv_{2}.zip'
    
    elif duration == 'hourly':
        url = 'https://www.cnrfc.noaa.gov/ensembleHourlyProductCSV.php'
        file_name = '{0}_{1}_hefs_csv_{2}.zip'
    
    if deterministic:
        url = 'https://www.cnrfc.noaa.gov/deterministicHourlyProductCSV.php'
        file_name = '{0}_{1}_csv_export.zip'

    # forecast datestamp prefix
    date_string = _default_date_string(date_string)
    
    # request table from ensemble product page and parse HTML
    soup = BeautifulSoup(_get_cnrfc_restricted_content(url), 'lxml')
    for td in soup.find_all('td', {'class': 'table-listing-content'}):
        if file_name.format(date_string, watershed, duration) in td.text:
            issue_time = parser.parse(td.next_sibling.text).astimezone(PACIFIC)
            return issue_time

    return None
    # raise ValueError('No valid issue time for URL.')


def get_watershed(cnrfc_id):
    """
    get associated hydrologic region for CNRFC forecast location
    """
    watersheds = {'klamath': KLAMATH_GAGES,
                  'NorthCoast': NORTHCOAST_GAGES,
                  'RussianNapa': RUSSIANNAPA_GAGES,
                  'UpperSacramento': UPPERSACRAMENTO_GAGES,
                  'FeatherYuba': FEATHERYUBA_GAGES,
                  'CachePutah': CACHEPUTAH_GAGES,
                  'american': AMERICAN_GAGES,
                  'LowerSacramento': LOWERSACRAMENTO_GAGES,
                  'CentralCoast': CENTRALCOAST_GAGES,
                  'SouthernCalifornia': SOUTHERNCALIFORNIA_GAGES,
                  'Tulare': TULARE_GAGES,
                  'SanJoaquin': SANJOAQUIN_GAGES,
                  'N_SanJoaquin': N_SANJOAQUIN_GAGES,
                  'EastSierra': EASTSIERRA_GAGES,
                  'Humboldt': HUMBOLDT_GAGES}

    for key, value in watersheds.items():
        if cnrfc_id.upper() in value:
            return key
    else:
        raise ValueError('cnrfc_id not recognized.')


def get_ensemble_first_forecast_ordinate(url=None, df=None):
    """
    return the first date of the forecast (GMT) as datetime object
    """
    if url is not None and df is None:
        df = pd.read_csv(url, 
                         nrows=1, 
                         header=0, 
                         skiprows=[1], 
                         parse_dates=[0], 
                         index_col=0, 
                         float_precision='high',
                         dtype={'GMT': str, 'FOLC1': float})

    return df.index.tolist()[0].to_pydatetime()


def get_ensemble_product_url(product_id, cnrfc_id, data_format=''):
    """
    return the URL for the product display
    """
    return 'https://www.cnrfc.noaa.gov/ensembleProduct{2}.php?id={1}&prodID={0}'.format(product_id, cnrfc_id, data_format)


def get_ensemble_product_1(cnrfc_id):
    """
    """
    raise NotImplementedError

    url = get_ensemble_product_url(1, cnrfc_id)
    get_web_status(url)
    return {'data': None, 'info': {'url': url, 
                                   'type': '10-Day Probability Plot',
                                   'units': 'TAF'}}


def get_ensemble_product_2(cnrfc_id):
    """
    http://www.cnrfc.noaa.gov/ensembleProduct.php?id=XXXC1&prodID=2

    (alt text source: https://www.cnrfc.noaa.gov/awipsProducts/RNOWRK10D.php)
    """
    url = get_ensemble_product_url(2, cnrfc_id)
    get_web_status(url)

    # request Ensemble Product 2 page content
    soup = BeautifulSoup(_get_cnrfc_restricted_content(url), 'lxml')
    data_table = soup.find_all('table', {'style': 'standardTable'})[0]

    # parse Tabular 10-Day Streamflow Volume Accumulation (1000s of Acre-Feet) from table
    df, notes = _parse_blue_table(data_table)
    df.set_index('Probability', inplace=True)

    # parse title and date updated from table
    for td in data_table.find_all('td', {'class': 'medBlue-background'}):
        title, time_issued = str(td.find('strong')).split('<br/>')
        time_issued = time_issued.rstrip('</strong>').lstrip('Data Updated: ')

    return {'data': df, 'info': {'url': url, 
                                 'type': 'Tabular 10-Day Streamflow Volume Accumulation',
                                 'issue_time': time_issued,
                                 'units': 'TAF',
                                 'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M')}}


def get_ensemble_product_3(cnrfc_id):
    """
    """
    raise NotImplementedError

    url = get_ensemble_product_url(3, cnrfc_id)
    get_web_status(url)
    return {'data': None, 'info': {'url': url, 
                                   'type': '5-Day Peaks Plot',
                                   'units': 'TAF'}}


def get_ensemble_product_5(cnrfc_id):
    """
    """
    raise NotImplementedError

    url = get_ensemble_product_url(5, cnrfc_id)
    get_web_status(url)
    return {'data': None, 'info': {'url': url, 
                                   'type': 'Tabular 5-Day Volume Accumulations',
                                   'units': 'TAF'}}


def get_ensemble_product_6(cnrfc_id):
    """
    Monthly exceedance volume data is presented in tabular format or plotted as a barchart at
    the ensemble product page
    """
    url = get_ensemble_product_url(6, cnrfc_id)
    get_web_status(url)

    # request Ensemble Product 6 page content
    soup = BeautifulSoup(_get_cnrfc_restricted_content(url), 'lxml')
    data_table = soup.find_all('table', {'style': 'standardTable'})[0]

    # parse Monthly Volume Exceedance Values from table
    df, notes = _parse_blue_table(data_table)
    df.set_index('Prob', inplace=True)

    # parse title and date updated from table
    for td in data_table.find_all('td', {'class': 'medBlue-background'}):
        title, time_issued = str(td.find('strong')).split('<br/>')
        time_issued = time_issued.rstrip('</strong>').lstrip('Data Updated: ')
        title = title.lstrip('<strong>')

    return {'data': df, 'info': {'url': url, 
                                 'type': title,
                                 'issue_time': time_issued,
                                 'units': 'TAF',
                                 'notes': notes,
                                 'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M')}}


def get_ensemble_product_10(cnrfc_id):
    """
    Water Year Accumulated Volume Plot - chart data available through highchart download CSV api
    Tabular Monthly Volume Accumulation

    @narlesky TO DO - recreate graphic
    """
    url = get_ensemble_product_url(10, cnrfc_id)
    get_web_status(url)

    # request Ensemble Product 10 page content
    soup = BeautifulSoup(_get_cnrfc_restricted_content(url), 'lxml')
    data_table = soup.find_all('table', {'style': 'standardTable'})[0]

    # parse Tabular 10-Day Streamflow Volume Accumulation (1000s of Acre-Feet) from table
    df, notes = _parse_blue_table(data_table)
    df.set_index('Probability', inplace=True)

    return {'data': df, 'info': {'url': url, 
                                 'note': '@narlesky TO DO - recreate graphic', 
                                 'type': 'Water Year Accumulated Volume Plot & Tabular Monthly Volume Accumulation',
                                 'units': 'TAF',
                                 'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M')}}


def get_ensemble_product_11(cnrfc_id):
    """
    Multi-Year Accumulated Volume Plot - chart data available through highchart download CSV api
    Tabular Monthly Volume Accumulation
    """
    raise NotImplementedError

    url = get_ensemble_product_url(11, cnrfc_id)
    get_web_status(url)
    return {'data': None, 'info': {'url': url, 
                                   'type': 'Multi-Year Accumulated Volume Plot & Tabular Monthly Volume Accumulation',
                                   'units': 'TAF'}}


def get_ensemble_product_12(cnrfc_id):
    """
    """
    raise NotImplementedError

    url = get_ensemble_product_url(12, cnrfc_id)
    get_web_status(url)
    return {'data': None, 'info': {'url': url, 
                                   'type': 'Historical Flows (Water Year & Seasonal (Apr-Jul)',
                                   'units': 'TAF'}}


def get_ensemble_product_13(cnrfc_id):
    """
    """
    raise NotImplementedError

    url = get_ensemble_product_url(13, cnrfc_id)
    get_web_status(url)
    return {'data': None, 'info': {'url': url, 
                                   'type': 'Water Resources Verification',
                                   'units': 'TAF'}}


def get_data_report_part_8():
    """
    https://www.wrh.noaa.gov/cnrfc/rsa_getprod.php?prod=RNORR8RSA&wfo=cnrfc&version=0
    """
    raise NotImplementedError

    url = 'https://www.wrh.noaa.gov/cnrfc/rsa_getprod.php?prod=RNORR8RSA&wfo=cnrfc&version=0'
    get_web_status(url)
    return {'data': None, 'info': {'url': url, 
                                   'type': 'Hydrology-meteorology Data Report Part 8',
                                   'units': 'TAF'}}


def get_monthly_reservoir_storage_summary():
    raise NotImplementedError

    url = 'https://www.cnrfc.noaa.gov/awipsProducts/RNORR6RSA.php'
    get_web_status(url)
    return {'data': None, 'info': {'url': url, 
                                   'type': 'CNRFC Monthly Reservoir Storage Summary',
                                   'units': 'TAF'}}


def esp_trace_analysis_wrapper():
    """
    """
    url = 'https://www.cnrfc.noaa.gov/esp_trace_analysis.php'


def _apply_conversions(df, duration, acre_feet, pdt_convert, as_pdt):

    # convert kcfs/day to cfs/day
    df = df * 1000.0
    units = 'cfs'

    if acre_feet:
        if duration == 'hourly':
            df = df * (3600 / 43560.0)
        elif duration == 'daily':
            df = df * (24 * 3600 / 43560.0)
        units = 'acre-feet'

    if pdt_convert:
        df.index = df.index.tz_localize('UTC').tz_convert('America/Los_Angeles')
        df.index.name = 'America/Los_Angeles'
    
    elif as_pdt:
        df.index = [PACIFIC.localize(x) for x in df.index]
        df.index.name = 'America/Los_Angeles'

    return df, units


def _get_cnrfc_restricted_content(url):
    """
    request page from CNRFC restricted site
    """
    basic_auth = requests.auth.HTTPBasicAuth(os.getenv('CNRFC_USER'), os.getenv('CNRFC_PASSWORD'))
    content = requests.get(url, auth=basic_auth).content
    return content


def _get_forecast_csv(url):
    """

    Arguments:
        url (str): the URL address for the specified forecast product
    Returns:
        csvdata (io.BytesIO): the forecast data as in-memory CSV
    """
    # data source
    filename = url.split('/')[-1]

    # check for credentials
    if not os.getenv('CNRFC_USER'):
        raise NotImplementedError(''.join(['Must specify CNRFC_USER and CNRFC_PASSWORD environment',
                                           ' variables for access to restricted site.']))

    # cnrfc authorization header
    basic_auth = requests.auth.HTTPBasicAuth(os.getenv('CNRFC_USER'), os.getenv('CNRFC_PASSWORD'))

    # initialize requests session with retries
    session = requests.Session()
    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    content = session.get(url, auth=basic_auth, verify=False).content

    # handle zipfiles
    if '.zip' in filename:

        # store content in memory and parse zipped file
        zipdata = io.BytesIO(content)
        zip_ref = zipfile.ZipFile(zipdata)

        # read filenames in zip archive
        csv_filename = zip_ref.namelist()[0]

        # extract CSV from zip object
        csvdata = io.BytesIO(zip_ref.read(csv_filename))
        zip_ref.close()
        zipdata.close()

    elif '.csv' in filename or '_csv' in filename:
        csvdata = io.BytesIO(content)

    return csvdata


def get_forecast_csvdata(url):
    return _get_forecast_csv(url)


def get_rating_curve(cnrfc_id):
    """
    returns paired flow and stage data parsed from the text of CNRFC rating curve JavaScript files
    
    Arguments:
        cnrfc_id (str): forecast point (such as FOLC1)
    Returns:
        result (dict): query result dictionary with 'data' and 'info' keys
    """
    # retrieve data from URL
    url = f'https://www.cnrfc.noaa.gov/data/ratings/{cnrfc_id}_rating.js'
    response = requests.get(url)

    # check if data exists
    if response.status_code == 200:
        raw_data = response.text.splitlines()

        # filter and extract flow and stage data
        flow_data = []
        stage_data = []
        for line in raw_data:
            if line.startswith('ratingFlow'):
                flow = line.split('(')[1].split(')')[0]
                flow_data.append(float(flow))
            elif line.startswith('ratingStage'):
                stage = line.split('(')[1].split(')')[0]
                stage_data.append(float(stage))

        # pair ratingFlow and ratingStage data
        data = list(zip(stage_data, flow_data))

    else:
        print(f'Error accessing rating curve URL for: {cnrfc_id}')
        data = None

    return {'data': data, 
            'info': {'url': url, 
                     'cnrfc_id': cnrfc_id}
            }


def _default_date_string(date_string):
    """
    supply expected latest forecast datestamp or use defined date_string argument
    """
    if date_string is None:
        now = dt.datetime.now().astimezone(UTC)
        date_string = now.strftime('%Y%m%d{0:02.0f}'.format(6 * math.floor(now.hour/6)))
    
    # hour validation
    if date_string[-2:] not in ['00', '06', '12', '18']:
        raise ValueError('date_string must be of form %Y%m%dXX, where XX is one of 00, 06, 12, 18.')
    
    return date_string


def _validate_duration(duration):
    if duration[0].upper() == 'D':
        return 'daily'
    elif duration[0].upper() == 'H':
        return 'hourly'
    else:
        raise ValueError('<duration> must be one of daily, hourly')


def _parse_blue_table(table_soup):
    """
    many CNRFC ensemble data products are stored in similarly-formatted tables
    """

    # parse header names from table
    columns = []
    for td in table_soup.find_all('td', {'class': 'blue-background'}):
        if bool(td.text.strip()):
            columns.append(td.text.strip())

    # parse data entries from table
    rows, notes = [], []
    for tr in table_soup.find_all('tr'):
        data_cells = tr.find_all('td', {'class': 'normalText'})
        if len(data_cells) > 1:
            row = []
            for td in data_cells:
                try:
                    row.append(float(td.text.strip()))
                except ValueError:
                    row.append(td.text.strip())
            rows.append(row)
        else:
            try:
                notes.append(data_cells[0].text.strip())
            except:
                pass

    # format as dataframe
    df = pd.DataFrame(rows, columns=columns).replace({'--': float('nan')})
    return df, notes


if __name__ == '__main__':

    # Folsom           | FOLC1
    # New Bullards Bar | NBBC1
    # Oroville         | ORDC1
    # Pine Flat        | PNFC1
    # Shasta           | SHDC1

    # print(get_ensemble_product_1('FOLC1'))
    # print(get_deterministic_forecast('SHDC1', truncate_historical=False)['data'].head())
    # print(get_ensemble_forecast('SHDC1', 'h')['data'].head())
    # print(get_deterministic_forecast_watershed('UpperSacramento', None)['data'].head())
    # print(get_ensemble_forecast_watershed('UpperSacramento', 'hourly', None)['data'].head())
    # print(get_seasonal_trend_tabular('SHDC1', 2018)['data'].head())


    # print(get_ensemble_product_2('ORDC1'))
    get_ensemble_product_6('ORDC1')
