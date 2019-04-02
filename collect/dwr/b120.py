# -*- coding: utf-8 -*-
import datetime as dt
import io
import re
from bs4 import BeautifulSoup
import pandas as pd
import requests

from collect.dwr import errors
from collect.utils import get_web_status, clean_fixed_width_headers


def get_b120_data(date_suffix=''):
    """
    B-120 Water Supply Forecast Summary
    for current (latest) B120 forecast, use date_suffix = ''
    for an earlier month, use format date_suffix = '_201804'
    """

    if validate_date_suffix(date_suffix, min_year=2017):

        # main B120 page (new DWR format)
        url = 'http://cdec.water.ca.gov/b120{}.html'.format(date_suffix)

        # parse HTML file structure; AJ forecast table
        soup = BeautifulSoup(requests.get(url).content, 'html5lib')
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
            wy_list.append([clean_td(td.text) for td in tr.find_all('td')])
        
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
            'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M')
        }
        return {'data': {'Apr-Jul': aj_df, 'WY': wy_df}, 'info': info}

    elif validate_date_suffix(date_suffix, min_year=2011):
        report_date = dt.datetime.strptime(date_suffix, '_%Y%m')
        return get_120_archived_reports(report_date.year, report_date.month)

    else:
        raise errors.B120SourceError('B120 Issuances before Feb. 2011 are available as PDFs.')


def validate_date_suffix(date_suffix, min_year=2017):
    """
    min year is 2017 for HTML-formatted report at 
        https://cdec.water.ca.gov/b120_YYYYMM.html
    min year is 2011 for text-formatted report at 
        http://cdec.water.ca.gov/reportapp/javareports?name=B120.YYYYMM
    """

    if date_suffix == '':
        return True

    elif dt.datetime.strptime(date_suffix, '_%Y%m') >= dt.datetime(min_year, 2, 1):
        return True
    
    else:
        return False


def clean_td(text):
    try:
        value = float(text.strip().replace('-', '').replace(',', '').replace('\xa0\xa0-', ''))
    except ValueError:
        value = text.strip()
    if value == '':
        value = None
    return value


def get_b120_update_data(date_suffix=''):
    
    # main B120 page (new DWR format)
    url = 'http://cdec.water.ca.gov/b120up{}.html'.format(date_suffix)

    if not validate_date_suffix(date_suffix, min_year=2018):
        raise errors.B120SourceError('B120 updates in this format not available before Feb. 2018.')

    # parse HTML file structure; AJ forecast table
    soup = BeautifulSoup(requests.get(url).content, 'lxml')
    tables = soup.find_all('table', {'class': 'doc-aj-table'})

    # unused header info
    thead = tables[0].find('thead').find('tr', {'class': 'header-row2'})
    headers = thead.find_all('th')  
    forecast_dates = [th.text for th in thead.find_all('th', {'class': 'header-fcast-dt'})]

    # read HTML table with April-July Forecast Updates (TAF)
    aj_list = []
    for table in tables:
        for tr in table.find('tbody').find_all('tr'):
            cells = tr.find_all('td')
            if len(cells) == 1:
                spans = cells[0].find_all('span')
                watershed = spans[0].text.strip()
                average = clean_td(spans[1].text.strip().split('= ')[-1])
            else:
                aj_list.append([watershed, average] + [clean_td(td.text) for td in cells])

    # dataframe storing Apr-Jul forecast table
    columns = ['Hydrologic Region', 'Average', 'Percentile']
    for date in forecast_dates:
        columns += ['{} AJ Vol'.format(date), '{} % Avg'.format(date)]

    df = pd.DataFrame(aj_list, columns=columns)

    info = {
        'url': url,
        'type': 'B120 Update',
        'title': soup.find('div', {'class': 'fts-doc-title'}).text, 
        'caption': soup.find('div', {'class': 'doc-table-caption'}).text, 
        'notes': soup.find('div', {'class': 'fts-doc-notes'}).text,
        'units': 'TAF',
        'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M')
    }

    return {'data': df, 'info': info}


def get_120_archived_reports(year, month):
    """
    Text-formatted reports available through CDEC javareports app for 2011-2017
    http://cdec.water.ca.gov/reportapp/javareports?name=B120.YYYYMM
    """

    report_date = dt.datetime(year, month, 1)

    if not validate_date_suffix(report_date.strftime('_%Y%m'), min_year=2011):
        raise errors.B120SourceError('B120 Issuances before Feb. 2011 are avilable as PDF.')

    url = f'http://cdec.water.ca.gov/reportapp/javareports?name=B120.{report_date:%Y%m}'
    
    result = requests.get(url).content  
    result = BeautifulSoup(result, 'lxml').find('pre').text
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
    wy_df = pd.read_fwf(buf, header=[1, 2, 3], skiprows=[4,])
    wy_df.dropna(inplace=True)

    # clean columns
    headers = clean_fixed_width_headers(wy_df.columns)
    headers[0] = 'Hydrologic Region'
    wy_df.columns = headers

    # separate 80% probability range
    wy_df['90% Exceedance'] = wy_df['80% Probability Range'].str.split('-', expand=True)[0]
    wy_df['10% Exceedance'] = wy_df['80% Probability Range'].str.split('-', expand=True)[1]

    # caption
    caption = []
    for x in  result.split('California Cooperative Snow Surveys')[1].split('(')[0].split('\r\n'):
        caption.append(x.strip())

    # notes
    notes = []
    for line in result.split('Notes:')[1].split('For more information')[0].replace(u'\xa0', ' ').split('\r\n')[1:]:
        if bool(line.strip()):
            notes.append(line.strip())
    notes = [x+'.' for x in ''.join(notes).split('.')]

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
    """
    columns = ['Hydrologic Region', 'Watershed', 'Apr-Jul Forecast', '% of Avg', '90% Exceedance', '10% Exceedance']
    df = pd.DataFrame(data_list, columns=columns)
    return df

