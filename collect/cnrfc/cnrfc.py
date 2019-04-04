# -*- coding: utf-8 -*-
import datetime as dt
import io
import os
import zipfile
from bs4 import BeautifulSoup
from dateutil import parser
from dotenv import load_dotenv
import pandas as pd
from pytz import timezone
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from collect.utils import clean_fixed_width_headers

UTC = timezone('UTC')
PACIFIC = timezone('America/Los_Angeles')
TODAY = dt.datetime.now().strftime('%Y%m%d')


# load credentials
load_dotenv()

# disable warnings in crontab logs
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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


def get_deterministic_forecast_watershed(watershed, date_string, cnrfc_id=None):
    """
    from: https://www.cnrfc.noaa.gov/deterministicHourlyProductCSV.php
    https://www.cnrfc.noaa.gov/csv/2019040318_american_csv_export.zip

    """
    units = 'kcfs'
    date_string = default_date_string(date_string)

    if date_string[-2:] not in ['00', '06', '12', '18']:
        raise ValueError('date_string must be of form %Y%m%d12.')

    # data source
    url = 'https://www.cnrfc.noaa.gov/csv/{0}_{1}_csv_export.zip'.format(date_string, watershed)
    filename = url.split('/')[-1].replace('.zip', '.csv')

    session = requests.Session()
    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    content = session.get(url, verify=False).content

    # store content in memory and parse zipped file
    zipdata = io.BytesIO(content)
    zip_ref = zipfile.ZipFile(zipdata)

    # extract CSV from zip object
    csvdata = io.BytesIO(zip_ref.read(filename.replace('.zip', '.csv')))       
    zip_ref.close()

    # parse forecast data from CSV
    df = pd.read_csv(csvdata, 
                     header=0, 
                     skiprows=[1,], 
                     nrows=60, 
                     parse_dates=True, 
                     index_col=0,
                     float_precision='high',
                     dtype={'GMT': str})

    # filter watershed for single forecast point ensemble
    if cnrfc_id is not None:
        columns = [x for x in df.columns if cnrfc_id in x]
    else:
        columns = df.columns
    
    # convert kcfs/day to acre-feet
    df = df[columns] * 1000.0 * ( 3600 * 24 / 43560.0 )
    units = 'cfs'

    # clean up
    zipdata.close()
    csvdata.close()

    # forecast issue time
    time_issued = get_watershed_forecast_issue_time('H', watershed, date_string, deterministic=True)

    return {'data': df, 'info': {'url': url, 
                                 'type': 'Deterministic Forecast', 
                                 'issue_time': time_issued.strftime('%Y-%m-%d %H:%M'),
                                 'watershed': watershed, 
                                 'units': units}}


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


def get_ensemble_forecast(cnrfc_id, duration, acre_feet=False, pdt_convert=False, as_pdt=False):
    """
    from: cnrfc_hourly_forecast_issue_time
          get_station_hourly_ensemble
          get_ensemble_first_forecast_ordinate

    ported from SAFCA Portal project
    possibly CNRFC is labeling GMT when it's actually already in PDT/PST??? - 13Feb2019
    date_string = time_issued.strftime('%Y%m%d_%H%M')
    """

    # default ensemble forecast units    
    units = 'kcfs'

    # validate duration
    if duration[0].upper() == 'H':
        duration = 'hourly'
    elif duration[0].upper() == 'D':
        duration = 'daily'
    else:
        raise ValueError('<duration> must be one of daily, hourly')

    # get issue time of most recent hourly inflow forecast (no support for daily yet)
    date_string = default_date_string(None)
    time_issued = get_watershed_forecast_issue_time(duration, get_watershed(cnrfc_id), date_string)

    # forecast data url
    url = 'https://www.cnrfc.noaa.gov/csv/{0}_hefs_csv_{1}.csv'.format(cnrfc_id, duration)

    # fetch hourly ensemble forecast data
    basic_auth = requests.auth.HTTPBasicAuth(os.getenv('CNRFC_USER'), os.getenv('CNRFC_PASSWORD'))
    session = requests.Session()
    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    content = session.get(url, auth=basic_auth, verify=False).text
    
    # read forecast ensemble series from CSV
    with io.StringIO(content) as csvdata:
        df = pd.read_csv(csvdata, 
                         header=0, 
                         skiprows=[1], 
                         parse_dates=[0], 
                         index_col=0, 
                         float_precision='high', 
                         dtype={'GMT': str, cnrfc_id: float})

    # rename columns for ensemble member IDs starting at 1950
    df.columns = [str(x) for x in range(1950, 1950 + len(df.columns))]
    
    # convert kcfs/day to cfs/day
    df = df * 1000.0
    units = 'cfs'

    if acre_feet:
        if duration == 'hourly':
            df = df * ( 3600 / 43560.0 )
        elif duration == 'daily':
            df = df * (24 * 3600 / 43560.0 )
        units = 'acre-feet'

    if pdt_convert:
        df.index = df.index.tz_localize('UTC').tz_convert('America/Los_Angeles')
        df.index.name = 'America/Los_Angeles'
    
    elif as_pdt:
        df.index = [PACIFIC.localize(x) for x in df.index]
        df.index.name = 'America/Los_Angeles'

    return {'data': df, 'info': {'url': url, 
                                 'type': '{0} Ensemble Forecast'.format(duration.title()),
                                 'units': units, 
                                 'issue_time': time_issued.strftime('%Y-%m-%d %H:%M')}}


def get_ensemble_forecast_watershed(watershed, duration, date_string, cnrfc_id=None):
    """
    from: get_watershed_ensemble_issue_time
          get_watershed_ensemble_daily

    download seasonal outlook for the watershed as zipped file, unzip...

    """
    units = 'kcfs'
    date_string = default_date_string(date_string)

    if date_string[-2:] != '12':
        raise ValueError('date_string must be of form %Y%m%d12.')

    # data source
    url = 'http://www.cnrfc.noaa.gov/csv/{0}_{1}_hefs_csv_{2}.zip'.format(date_string, watershed, duration)
    filename = url.split('/')[-1].replace('.zip', '.csv')

    session = requests.Session()
    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    content = session.get(url, verify=False).content

    # store content in memory and parse zipped file
    zipdata = io.BytesIO(content)
    zip_ref = zipfile.ZipFile(zipdata)

    # extract CSV from zip object
    csvdata = io.BytesIO(zip_ref.read(filename.replace('.zip', '.csv')))       
    zip_ref.close()

    # get date/time stamp from ensemble download page
    try:
        time_issued = get_watershed_forecast_issue_time(duration, watershed, date_string)
    except:
        time_issued = UTC.localize(dt.datetime.strptime(date_string, '%Y%m%d12')).strftime('%Y-%m-%d 12:00')

    # parse forecast data from CSV
    df = pd.read_csv(csvdata, 
                     header=0, 
                     skiprows=[1,], 
                     nrows=60, 
                     parse_dates=True, 
                     index_col=0,
                     float_precision='high',
                     dtype={'GMT': str})#, 'FOLC1': float})

    # filter watershed for single forecast point ensemble
    if cnrfc_id is not None:
        columns = [x for x in df.columns if cnrfc_id in x]
    else:
        columns = df.columns
    
    # convert kcfs/day to acre-feet
    df = df[columns] * 1000.0 * ( 3600 * 24 / 43560.0 )
    units = 'cfs'

    # clean up
    zipdata.close()
    csvdata.close()

    return {'data': df, 'info': {'url': url, 
                                 'watershed': watershed, 
                                 'issue_time': issue_time,
                                 'units': units, 
                                 'duration': duration}}


def get_watershed_forecast_issue_time(duration, watershed, date_string=None, deterministic=False):
    """
    get "last modified" date/time stamp from CNRFC watershed ensemble product table
    """
    if duration[0].upper() == 'D':
        #" only on the 12"
        date_string = date_string[:-2] + '12'
        url = 'https://www.cnrfc.noaa.gov/ensembleProductCSV.php'
        duration = 'daily'
        file_name = '{0}_{1}_hefs_csv_{2}.zip'
    
    elif duration[0].upper() == 'H':
        url = 'https://www.cnrfc.noaa.gov/ensembleHourlyProductCSV.php'
        duration = 'hourly'
        file_name = '{0}_{1}_hefs_csv_{2}.zip'
    
    if deterministic:
        url = 'https://www.cnrfc.noaa.gov/deterministicHourlyProductCSV.php'
        file_name = '{0}_{1}_csv_export.zip'

    date_string = default_date_string(date_string) 
    content = requests.get(url, verify=False).content
    soup = BeautifulSoup(content, 'lxml')
    
    for td in soup.find_all('td', {'class': 'table-listing-content'}):
        if file_name.format(date_string, watershed, duration) in td.text:
            issue_time = parser.parse(td.next_sibling.text).astimezone(PACIFIC)
            return issue_time


def get_watershed(cnrfc_id):
    """
    get associated hydrologic region for CNRFC forecast location
    """
    if cnrfc_id.upper() in ['AKYC1', 'AKYC1F', 'BCTC1', 'CBAC1', 'CBAC1F', 'CBAC1L', 'FMDC1', 'FMDC1O', 'FOLC1', 'FOLC1F', 'HLLC1', 'HLLC1F', 'HLLC1L', 'HLLC1SPL', 'ICHC1', 'LNLC1', 'LNLC1F', 'MFAC1', 'MFAC1F', 'MFAC1L', 'NFDC1', 'NMFC1', 'RBBC1F', 'RBBC1SPL', 'RRGC1', 'RRGC1F', 'RRGC1L', 'RUFC1', 'RUFC1L', 'SVCC1', 'SVCC1F', 'SVCC1L', 'UNVC1', 'UNVC1F']:
        return 'american'
    elif cnrfc_id.upper() in ['DLTC1', 'MMCC1', 'MSSC1', 'MSSC1F', 'CNBC1', 'PITC1F', 'SHDC1', 'PLYC1', 'CWAC1', 'COTC1', 'CWCC1', 'WHSC1', 'EDCC1', 'MLMC1', 'TCRC1', 'DCVC1', 'HKCC1', 'BKCC1', 'EPRC1', 'SGEC1', 'BLBC1', 'BDBC1', 'RDBC1', 'TEHC1', 'VWBC1', 'HAMC1', 'ORFC1', 'BTCC1', 'CLSC1', 'CLAC1', 'CLUC1', 'TISC1', 'WLKC1', 'RDBC1L', 'TEHC1L', 'BDBC1L', 'RDBC1L2', 'VWBC1L', 'ORFC1L']:
        return 'UpperSacramento'
    else:
        return None


def default_date_string(date_string):
    if date_string is None:
        now = dt.datetime.today()
        date_string = now.strftime('%Y%m%d{0}'.format(6 * round(now.hour//6)))
    return date_string


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


if __name__ == '__main__':

    # from pprint import pprint
    RESERVOIRS = {'Folsom': 'FOLC1',
                  'New Bullards Bar': 'NBBC1',
                  'Oroville': 'ORDC1',
                  'Pine Flat': 'PNFC1',
                  'Shasta': 'SHDC1'}

    # pprint(get_deterministic_forecast('SHDC1', truncate_historical=False)['info'])

    # print(get_ensemble_forecast('SHDC1', 'd')['data'].head())

    # print(get_deterministic_forecast_watershed('american', None)['info'])

    # print(get_ensemble_forecast_watershed('american', 'hourly', None)['data'].head())

    # print(get_seasonal_trend_tabular('SHDC1', 2018)['data'])