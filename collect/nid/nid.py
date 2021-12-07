"""
collect.nid.nid
============================================================
access NID flow and volume daily data

Available Sites (generate with `nid.get_sites()`)
    Auburn Ravine I at Head (BR100)
    Hemphill Canal at Head (BR220)
    Combie Phase I at Head (BR301)
    Camp Far West at Head (BR334)
    Gold Hill I at Head (BR368)
    Combie Reservoir-Spill-1600. (BR900)
    Bowman-Spaulding Canal Intake Near Graniteville, Ca (BSCA)
    Bowman Lake Near Graniteville, Ca (BWMN)
    Chicago Park Flume Near Dutch Flat, Ca (CPFL)
    Cascade at Head (DC102)
    Newtown Canal at Head (DC131)
    Tunnel Canal at Head (DC140)
    D. S. Canal at Head (DC145)
    Tarr Canal at Head (DC169)
    Scott''s Flat Reservoir (DC900)
    Dutch Flat #2 Flume Near Blue Canyon, Ca (DFFL)
    Faucherie Lake Near Cisco, Ca (FAUC)
    French Lake Near Cisco Grove, Ca (FRLK)
    Jackson Lake near Sierra City (JKSN)
    Jackson Meadows Reservoir Near Sierra City, Ca (JMDW)
    Milton-Bowman Tunnel Outlet (South Portal) (MBTO)
    Rollins Reservoir Near Colfax, Ca (ROLK)
    Sawmill Lake Near Graniteville, Ca (SWML)
    Wilson Creek near Sierra City (WLSN)
"""
# -*- coding: utf-8 -*-
import datetime as dt
import io
import re

from bs4 import BeautifulSoup
import pandas as pd
import requests


def get_sites():
    """
    reads hyquick index and returns dictionary of included sites

    Returns:
        sites (dict): dictionary of site IDs and titles
    """
    url = "https://river-lake.nidwater.com/hyquick/index.htm"
    df = pd.read_html(requests.get(url).content, header=1, index_col=0)[0]
    sites = df.to_dict()['Name']
    return sites


def get_issue_date():
    """
    reads timestamp on hyquick index page and returns as a datetime

    Returns:
        issue_date (datetime.datetime): the last update of the NID hyquick page
    """
    url = "https://river-lake.nidwater.com/hyquick/index.htm"
    df = pd.read_html(requests.get(url).content, header=None)[0]
    return dt.datetime.strptime(df.iloc[0, 1], 'Run on %Y/%m/%d %H:%M:%S')


def get_site_files(site):
    """
    Arguments:
        site (str): the site id
        interval (str): the site interval
    Returns:
        links (list): sorted list of linked files available for site
    """
    url = get_station_url(site, metric='index')
    soup = BeautifulSoup(requests.get(url).content, 'lxml')
    links = {a.get('href') for a in soup.find_all('a')}
    return sorted(links)


def get_site_metric(site, interval='daily'):
    """
    Arguments:
        site (str): the site id
        interval (str): the site interval
    Returns:
        metric (str): 
    """
    # daily or hourly search pattern
    search_pattern = r'usday_daily_(.*?)\.txt' if interval == 'daily' else r'csv_(.*?)\.csv'

    # loop through available links to determine product matching interval
    for link in get_site_files(site):
        matches = re.search(search_pattern, link)
        if matches is not None:
            return matches.group(1)


def get_station_url(site, metric='index', interval=None):
    """
    Arguments:
        site (str): the id for NID hydstra entry
        metric (str): the data metric; default is 'index', which provides url to site index
        interval (str): data interval, one of daily or hourly
    Returns:
        url (str): the hyquick url for request
    """
    if metric == 'index':
        return f'https://river-lake.nidwater.com/hyquick/{site}/index.htm'

    if interval == 'daily':
        return f'https://river-lake.nidwater.com/hyquick/{site}/{site}.usday_daily_{metric}.txt'
    
    elif interval == 'hourly':
        return f'https://river-lake.nidwater.com/hyquick/{site}/{site}.csv_{metric}.csv'


def get_daily_data(site, json_compatible=False):
    """
    returns a `dict` and creates JSON file of the data and info for NID sites provided

    Arguments:
        site (str): NID site identifier
        water_year (int): current water year
    Returns:
        result (dict): dictionary of data and info for each site
    """
    metric = get_site_metric(site, interval='daily')
    url = get_station_url(site, metric=metric, interval='daily')
    response = requests.get(url).text

    frames = []
    for group in re.split(r'(?=Nevada Irrigation District\s+)', response):

        if not bool(group):
            continue

        # split by start of table header line
        pre_table, table = re.split(r'(?=Day\s{2,}OCT)', group)

        # get water year, site info for water year table
        meta = get_daily_meta(content=pre_table)

        # load water year table to dataframe
        data = pd.read_fwf(io.StringIO(re.split(r'\nTotal', table)[0]), 
                           header=0, 
                           skiprows=[1], 
                           nrows=36,
                           na_values=['------', 'NaN', '']).dropna(how='all')

        # convert from monthly table to water-year series
        df = pd.melt(data, id_vars='Day').rename({'variable': 'month', 'value': metric}, axis=1)

        # assign calendar year to each entry
        df['year'] = df['month'].apply(lambda x: meta['water_year'] -1 if x in ['OCT', 'NOV', 'DEC'] else meta['water_year'])
        df.index = df['Day'].astype(str) + df['month'] + df['year'].astype(str)

        # drop non-existent date entries (i.e. 31NOVYYYY)
        df.dropna(inplace=True)

        # convert index to datetimes
        df.index = pd.to_datetime(df.index)
        df = df.reindex(pd.date_range(start=df.index[0], end=df.index[-1]))

        frames.append(df)

    # return the dataset
    df = pd.concat(frames, axis=0)
    return {'info': {'site': site,
                     'description': meta['Site'], 
                     'usgs_id': meta['USGS #'], 
                     'district': meta['district'],
                     'version': meta['version'],
                     'report_stamp': meta['report_stamp'],
                     'url': url,
                     'metric': metric, 
                     'timeseries_type': {'flow': 'flows', 'volume': 'storages'}.get(metric),
                     'timeseries_units': {'flow': 'cfs', 'volume': 'AF'}.get(metric)}, 
            'data': serialize(df[[metric]]) if json_compatible else df[[metric]]}


def get_daily_meta(url=None, content=None):
    """
    Arguments:
        url (str): the site data url
    Returns:
        result (dict): meta data from the content above the daily fixed-width data table
    """
    if url:
        data = [re.sub(r'\s{2,}|:\s+|:', '|', x.strip()).split('|') 
                for x in requests.get(url).text.splitlines()[:10]]
    elif content:
        data = [re.sub(r'\s{2,}|:\s+|:', '|', x.strip()).split('|') 
                for x in content.splitlines()]

    result = {'district': data[0][0], 'version': data[0][1], 'report_stamp': data[0][2]}
    for row in data:
        if len(row) == 2:
            result.update({row[0]: row[1]})

    # extract water year from end date entry
    result.update({'water_year': dt.datetime.strptime(result['Ending Date'], '%m/%d/%Y').year})
    return result


def get_hourly_data(site, json_compatible=False):
    """
    returns a `dict` and creates JSON file of the data and info for NID sites provided

    Arguments:
        site (str): NID site identifier
    Returns:
        result (dict): dictionary of data and info for each site
    """
    metric = get_site_metric(site, interval='hourly')
    url = get_station_url(site, metric=metric, interval='hourly')
    df = pd.read_csv(url, header=1, na_values=[' ""', 'nan', '', ' '])

    # clean up extra spaces in column names
    df.columns = df.columns.map(lambda x: x.strip())

    # get the qualifiers table
    qualifiers = parse_qualifiers(df.pop('Site Information'))

    # set data types
    df = df.astype({'Date': str,
                    'Time': str,
                    'Measured Value': float,
                    'Units': str,
                    'Amount Diverted (AF)': float,
                    'Quality': int})

    # convert to date/time index
    df.index = pd.to_datetime(df['Date']+df['Time'])

    # remove extra whitespace in data entries
    df.loc[:, 'Time'] = df.loc[:, 'Time'].str.strip()
    df.loc[:, 'Units'] = df.loc[:, 'Units'].str.strip()

    # return the dataset
    return {'info': {'site': site,
                     'url': url,
                     'metric': metric,
                     'qualifiers': qualifiers,
                     'timeseries_type': {'flow': 'flows', 'volume': 'storages'}.get(metric),
                     'timeseries_units': {'flow': 'cfs', 'volume': 'AF'}.get(metric)}, 
            'data': serialize(df) if json_compatible else df}


def parse_qualifiers(series):
    """
    Arguments:
        series (pandas.Series): Site Information column from hyquick site
    Returns:
        (dict): dictionary of data qualifiers
    """
    entries = series.dropna().tolist()
    entries.remove('Qualities:')
    return {x: y for x, y in map(lambda x: x.split(' - '), entries)}


def serialize(df, day_format='%Y-%-m-%-d'):
    """
    Arguments:
        df (pandas.DataFrame): a pandas dataframe with date/time index
    Returns:
        df (pandas.DataFrame): a pandas dataframe formatted for export to JSON
    """
    df.index = df.index.strftime(f'{day_format} %H:%M')
    df.fillna('null', inplace=True)
    return df.to_dict()
