"""
collect.dwr.b120
============================================================
access DWR Bulletin 120 forecast data

TODO - add support for historical reports in format: 
    https://cdec.water.ca.gov/reportapp/javareports?name=B120.201203
    https://cdec.water.ca.gov/reportapp/javareports?name=B120.201802

TODO - check updated homepage for bulletin 120 for new links
    https://cdec.water.ca.gov/snow/bulletin120/index2.html
    tie validation of dates to https://cdec.water.ca.gov/prev_b120.html and https://cdec.water.ca.gov/prev_b120up.html
"""
# -*- coding: utf-8 -*-
import datetime as dt
import io
import re

from bs4 import BeautifulSoup
import pandas as pd
import requests

from collect.dwr import errors


def get_b120_data(date_suffix=''):
    """
    B-120 Water Supply Forecast Summary
    for current (latest) B120 forecast, use date_suffix = ''
    for an earlier month, use format date_suffix = '_201804'

    Args:
        date_suffix (str):
    Returns:
        (dict): dictionary of extracted data and metadata (info)
    Raises:
        collect.dwr.errors.B120SourceError: raised when the specified date is outside of the range available as HTML products
    """
    if validate_date_suffix(date_suffix, min_year=2017):

        # main B120 page (new DWR format)
        url = 'https://cdec.water.ca.gov/b120{}.html'.format(date_suffix)

        # parse HTML file structure; AJ forecast table
        soup = BeautifulSoup(requests.get(url).content, 'html.parser')
        table = soup.find('table', {'class': 'doc-aj-table'})

        # read HTML table with April-July Forecast Summary (TAF)
        aj_list = []
        for tr in table.find('tbody').find_all('tr'):
            cells = tr.find_all('td')
            if len(cells) == 1:
                watershed = cells[0].text.strip()
            else:
                aj_list.append([watershed] + [clean_td(td.text) for td in cells])

        # dataframe storing Apr-Jul forecast table
        aj_df = april_july_dataframe(aj_list)

        # water-year (wy) forecast summary and monthly distribution (TAF)
        table = soup.find('table', {'class': 'doc-wy-table'})

        # read HTML table with Water-Year Forecast Summary
        wy_list = []
        for tr in table.find('tbody').find_all('tr'):            
            clean_row = [clean_td(td.text) for td in tr.find_all('td')]
            if clean_row[0] == 'Download in comma-delimited format':
                continue
            wy_list.append(clean_row)
        
        # header info
        headers = table.find('thead').find('tr', {'class': 'header-row2'}).find_all('th')
        columns = [th.text.replace('thru', '-').replace('%', ' % ').replace('WaterYear', 'WY') for th in headers]
        columns = columns[:-2] + ['90% Exceedance', '10% Exceedance'] + [columns[-1]]
        wy_df = pd.DataFrame(wy_list, columns=columns)

        info = {
            'url': url,
            'type': 'B120 Forecast',
            'title': soup.find('div', {'class': 'fts-doc-title'}).text, 
            'caption': soup.find('div', {'class': 'doc-table-caption'}).text.replace('FORECASTOF', 'FORECAST OF'),
            'notes': soup.find('div', {'class': 'doc-fcast-notes'}).text.replace(u'\xa0', ' ').split('\n'),
            'units': 'TAF',
            'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M'),
        }

        info.update({'posted': dt.datetime.strptime(info['title']
                                                    .split('(posted on ')[1]
                                                    .rstrip(')'), 
                                                    '%m/%d/%y %H:%M') })

        return {'data': {'Apr-Jul': aj_df, 'WY': wy_df}, 'info': info}

    elif validate_date_suffix(date_suffix, min_year=2011):
        report_date = dt.datetime.strptime(date_suffix, '_%Y%m')
        return get_120_archived_reports(report_date.year, report_date.month)

    else:
        raise errors.B120SourceError('B120 Issuances before Feb. 2011 are available as PDFs.')


def validate_date_suffix(date_suffix, min_year=2017):
    """
    min year is 2017 for HTML-formatted report at https://cdec.water.ca.gov/b120_YYYYMM.html
    min year is 2011 for text-formatted report at https://cdec.water.ca.gov/reportapp/javareports?name=B120.YYYYMM

    Args:
        date_suffix (str): string date suffix representing year and month (_YYYYMM)
        min_year (int): the minimum year for valid date suffix format
    Returns:
        (bool): flag to indicate whether provided date_suffix is valid
    """
    if date_suffix == '':
        return True

    elif dt.datetime.strptime(date_suffix, '_%Y%m') >= dt.datetime(min_year, 2, 1):
        return True

    return False


def clean_td(text):
    """
    Args:
        text (str):
    Returns:
        value (float, None)
    """
    try:
        value = float(text.strip().replace('-', '').replace(',', '').replace('\xa0\xa0-', ''))
    except ValueError:
        value = text.strip()
    if value == '':
        value = None
    return value


def get_b120_update_data(date_suffix=''):
    """
    Args:
        date_suffix (str): optional 

    Returns:

    """

    # main B120 page (format circa 2020)
    url = 'https://cdec.water.ca.gov/b120up.html'

    if not validate_date_suffix(date_suffix, min_year=2018):
        raise errors.B120SourceError('B120 updates in this format not available before Feb. 2018.')

    # parse HTML file structure; AJ forecast table
    soup = BeautifulSoup(requests.get(url).content, 'html.parser')
    tables = soup.find_all('table', {'class': 'doc-aj-table'})

    # unused header info
    thead = tables[0].find('thead').find('tr', {'class': 'header-row2'})
    headers = thead.find_all('th')  
    forecast_dates = [th.text for th in thead.find_all('th', {'class': 'header-fcast-dt'})]

    # read HTML table with April-July Forecast Updates (TAF)
    aj_list = []
    for table in tables:
        watershed = None
        average = None

        for tr in table.find('tbody').find_all('tr'):        
            cells = tr.find_all('td')

            clean_row = [clean_td(td.text) for td in cells]
            if clean_row[0] == 'Download in comma-delimited format':
                continue                

            if cells[0]['class'][0] == 'col-basin-name':
                spans = cells[0].find_all('span')
                watershed = spans[0].text.strip()
                average = clean_td(spans[1].text.strip().split('= ')[-1])
                continue

            row_formatted = [watershed, average] + clean_row
            aj_list.append(row_formatted)

    # dataframe storing Apr-Jul forecast table
    columns = ['Hydrologic Region', 'Average', 'Percentile']
    for date in forecast_dates:
        columns += ['{} AJ Vol'.format(date), '{} % Avg'.format(date)]

    df = pd.DataFrame(aj_list, columns=columns)

    title = soup.find('div', {'class': 'fts-doc-title'}).text

    info = {
        'url': url,
        'type': 'B120 Update',
        'title': soup.find('div', {'class': 'fts-doc-title'}).text, 
        'caption': soup.find('div', {'class': 'doc-table-caption'}).text, 
        'notes': soup.find('div', {'class': 'fts-doc-notes'}).text,
        'units': 'TAF',
        'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M'),
    }

    # extract date/time from posted date
    info.update({'posted': dt.datetime.strptime(info['title']
                                                .split('(posted on ')[1]
                                                .rstrip(')'), 
                                                '%m/%d/%y %H:%M')})

    return {'data': df, 'info': info}


def get_120_archived_reports(year, month):
    """
    Text-formatted reports available through CDEC javareports app for 2011-2017
    https://cdec.water.ca.gov/reportapp/javareports?name=B120.YYYYMM
    
    Args:
        year (int): the year as 4-digit integer
        month (int): the month as integer from 1 to 12
    Returns:
        (dict): nested dictionary with two result dataframes and metadata
    """
    report_date = dt.datetime(year, month, 1)

    if not validate_date_suffix(report_date.strftime('_%Y%m'), min_year=2011):
        raise errors.B120SourceError('B120 Issuances before Feb. 2011 are avilable as PDF.')

    url = f'https://cdec.water.ca.gov/reportapp/javareports?name=B120.{report_date:%Y%m}'
    
    result = requests.get(url).content  
    result = BeautifulSoup(result, 'html.parser').find('pre').text
    tables = result.split('Water-Year (WY) Forecast and Monthly Distribution')

    # read text table with April-July Forecast Summary (TAF)
    table = tables[0].split('April-July Forecast')[1].split('-'*80)[1]
    aj_list = []
    for row in table.splitlines():
        cells = re.split(r'\s{2,}', row)
        if len(cells) == 1:
            watershed = cells[0].strip()
        else:
            aj_list.append([watershed] + [clean_td(td) for td in cells][1:])

    # dataframe storing Apr-Jul forecast table
    aj_df = april_july_dataframe(aj_list)
    aj_df.dropna(subset=['Hydrologic Region'], inplace=True)

    # water-year (wy) forecast summary and monthly distribution (TAF)
    table = tables[1].split('Notes')[0]

    # in-memory file buffer
    buf = io.StringIO()
    buf.write(table)
    buf.seek(0)

    # parse fixed-width file
    wy_df = pd.read_fwf(buf, header=None, skiprows=[0, 1, 2, 3, 4])

    # assign column names
    wy_df.columns = ['Hydrologic Region', 'Oct thru Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul',
                     'Aug', 'Sep', 'Water Year', '90% Exceedance', 'to', '10% Exceedance', 'WY % Avg']
    wy_df.dropna(inplace=True)
    wy_df.drop('to', axis=1, inplace=True)

    # caption
    caption = []
    for x in  result.split('California Cooperative Snow Surveys')[1].split('(')[0].split('\r\n'):
        caption.append(x.strip())

    # notes
    notes = []
    for line in result.split('Notes:')[1].split('For more information')[0].replace(u'\xa0', ' ').split('\r\n')[1:]:
        if bool(line.strip()):
            notes.append(line.strip())
    notes = [x + '.' for x in ''.join(notes).split('.')]

    info = {
        'url': url,
        'type': 'B120 Forecast',
        'title': result.splitlines()[1], 
        'caption': ''.join(caption).replace('FORECASTOF', 'FORECAST OF'),
        'notes': ['Notes:'] + notes,
        'units': 'TAF',
        'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M')
    }

    return {'data': {'Apr-Jul': aj_df, 'WY': wy_df}, 'info': info}


def april_july_dataframe(data_list):
    """
    dataframe storing Apr-Jul forecast table

    Args:
        data_list (list):

    Returns:
        
    """
    columns = ['Hydrologic Region', 'Watershed', 'Apr-Jul Forecast', '% of Avg', '90% Exceedance', '10% Exceedance']
    df = pd.DataFrame(data_list, columns=columns)
    return df
