"""
collect.dwr.b120
============================================================
access DWR Bulletin 120 forecast data
"""
# -*- coding: utf-8 -*-
import datetime as dt
import io
import re
from bs4 import BeautifulSoup
import pandas as pd
import requests

from collect.dwr import errors
from collect.utils.utils import get_web_status, clean_fixed_width_headers


# TODO - add support for historical reports in format: 
# http://cdec.water.ca.gov/reportapp/javareports?name=B120.201203
# http://cdec.water.ca.gov/reportapp/javareports?name=B120.201802

# TODO - check updated homepage for bulletin 120 for new links
# https://cdec.water.ca.gov/snow/bulletin120/index2.html

# tie validation of dates to https://cdec.water.ca.gov/prev_b120.html and https://cdec.water.ca.gov/prev_b120up.html

def get_b120_data(date_suffix=''):
    """
    B-120 Water Supply Forecast Summary
    for current (latest) B120 forecast, use date_suffix = ''
    for an earlier month, use format date_suffix = '_201804'

    Args:
        date_suffix (str):
    Returns:

    """

    if validate_date_suffix(date_suffix, min_year=2021):
        report_date = dt.datetime.strptime(date_suffix, '_%Y%m%d')
        return get_120_new_format(report_date.year, report_date.month, report_date.day)

    elif validate_date_suffix(date_suffix, min_year=2017):

        # main B120 page (new DWR format)
        url = 'http://cdec.water.ca.gov/b120{}.html'.format(date_suffix[0:-2])

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
            'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M'),
        }

        info.update({'posted': dt.datetime.strptime(info['title']
                                                    .split('(posted on ')[1]
                                                    .rstrip(')'), 
                                                    '%m/%d/%y %H:%M') })

        return {'data': {'Apr-Jul': aj_df, 'WY': wy_df}, 'info': info}

    elif validate_date_suffix(date_suffix, min_year=2011):
        report_date = dt.datetime.strptime(date_suffix, '_%Y%m%d')
        return get_120_archived_reports(report_date.year, report_date.month)

    else:
        raise errors.B120SourceError('B120 Issuances before Feb. 2011 are available as PDFs.')


def validate_date_suffix(date_suffix, min_year=2017):
    """
    min year is 2017 for HTML-formatted report at 
        https://cdec.water.ca.gov/b120_YYYYMM.html
    min year is 2011 for text-formatted report at 
        http://cdec.water.ca.gov/reportapp/javareports?name=B120.YYYYMM

    Args:
        date_suffix (str):
        min_year (int):
    Returns:

    """

    if date_suffix == '':
        return True

    elif dt.datetime.strptime(date_suffix, '_%Y%m%d') >= dt.datetime(min_year, 2, 1):
        return True
    
    else:
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


def get_b120_update_data(date_suffix=''): # need to redo this
    """
    Args:
        date_suffix (str):

    Returns:

    """

    # main B120 page (new DWR format)
    # available Feb, Mar, Apr, May, Jun
    url = 'http://cdec.water.ca.gov/b120up{}.html'.format(date_suffix[0:-2])

    if validate_date_suffix(date_suffix, min_year=2021):
        report_date = dt.datetime.strptime(date_suffix, '_%Y%m%d')
        return get_120_update_data_new_format(report_date.year, report_date.month)

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


def get_120_update_data_new_format(year, month): 
    """
    Args:
        date_suffix (str):

    Returns:

    """

    # main B120 page (new DWR format)
    report_date = dt.datetime(year, month, 1)

    # available Feb, Mar, Apr, May, Jun
    url = f'https://cdec.water.ca.gov/reportapp/javareports?name=B120UP.{report_date:%Y%m}'

    if not validate_date_suffix(report_date.strftime('_%Y%m%d'), min_year=2011):
        raise errors.B120SourceError('B120 Issuances before Feb. 2011 are avilable as PDF.')

    result = requests.get(url).content
    soup = BeautifulSoup(requests.get(url).content, 'lxml')
    tables = soup.find_all('table', {'class': 'data', 'id': 'B120UP'})

    # # unused header info
    thead = tables[0].find_all('th', {'class': 'top center'})
    forecast_dates = [th.text for th in thead[0:3]]

    # read HTML table with April-July Forecast Updates (TAF)
    aj_list = []
    row_list = []
    watershed = None
    average = None

    for table in tables:
        for tr in table.find_all('tr'):

            cells = tr.find_all('td')

            if len(cells) == 9:
                for i, cell in enumerate(cells):
                    cell_text = cell.text.strip()

                    if i == 0:
                        row_list = []
                        row_list.append(watershed)
                        row_list.append(average)

                    if 'Average' in str(cell.text):
                        watershed = cells[0].text.strip()
                        average = cell_text.split('= ')[-1]

                    else:
                        if cell_text == '':
                            pass
                        else:
                            row_list.append(cell_text)

                # do not add lists that do not have each column
                if len(row_list)==9:
                    aj_list.append(row_list)

    # dataframe storing Apr-Jul forecast table
    columns = ['Hydrologic Region', 'Average', 'Percentile']
    for date in forecast_dates:
        columns += ['{} AJ Vol'.format(date), '{} % Avg'.format(date)]

    df = pd.DataFrame(aj_list, columns=columns)

    info = {
        'url': url,
        'type': 'B120 Update',
        'title': soup.find('h1').text, 
        'caption': soup.find('h3').text, 
        'notes': soup.find('table', {'class': 'data', 'id': 'NOTES'}).text.replace(u'\xa0', ' ').split('\n')[1],
        'units': 'TAF',
        'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M'),
    }

    info.update({'posted': dt.datetime.strptime(soup.find('left').text
                                                .split('Report generated: ')[1]
                                                .rstrip(')'), 
                                                '%B %d, %Y %H:%M') })

    return {'data': df, 'info': info}


def get_120_archived_reports(year, month):
    """
    Text-formatted reports available through CDEC javareports app for 2011-2017
    http://cdec.water.ca.gov/reportapp/javareports?name=B120.YYYYMM
    
    Args:
        year (int):
        month (int):

    Returns:
        (dict)
    """

    report_date = dt.datetime(year, month, 1)

    if not validate_date_suffix(report_date.strftime('_%Y%m%d'), min_year=2011):
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
    wy_df = pd.read_fwf(buf, header=None, skiprows=[0,1,2,3,4,])
    wy_df.dropna(inplace=True)

    # clean columns
    headers = ['Hydrologic Region','Oct thru Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Water Year','90%% Exceedance','Delete','10%% Exceedance','WY %% Avg']
    wy_df.columns = headers

    wy_df.drop(['Delete'], axis=1, inplace=True)
    wy_df['80%% Probability Range'] = wy_df['90%% Exceedance'].map(str) + ' - ' + wy_df['10%% Exceedance'].map(str)

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


def get_120_new_format(year, month, day):
    """
    Text-formatted reports available through CDEC javareports app for 2011-2017
    http://cdec.water.ca.gov/reportapp/javareports?name=B120.YYYYMM
    
    Args:
        year (int):
        month (int):

    Returns:
        (dict)
    """

    report_date = dt.datetime(year, month, day)

    if not validate_date_suffix(report_date.strftime('_%Y%m%d'), min_year=2011):
        raise errors.B120SourceError('B120 Issuances before Feb. 2011 are avilable as PDF.')

    url = f'http://cdec.water.ca.gov/reportapp/javareports?name=B120.{report_date:%Y%m}'

    result = requests.get(url).content
    soup = BeautifulSoup(requests.get(url).content, 'lxml')
    tables = soup.find_all('table', {'class': 'data', 'id': 'B120'})

    # read HTML table with April-July Forecast Summary (TAF)
    aj_list = []
    for table in tables:
        for i, tr in enumerate(table.find_all('tr')):
            cells = tr.find_all('td')
            if i == 1: # skip title line
                pass
            else:
                if len(tr) == 1:
                    region = tr.text.strip()
                else:
                    aj_list.append([region] + [clean_td(td.text) for td in cells])

    # dataframe storing Apr-Jul forecast table
    aj_df = april_july_dataframe(aj_list)
    aj_df.dropna(subset=['Hydrologic Region'], inplace=True)

    info = {
        'url': url,
        'type': 'B120 Forecast',
        'notes': soup.find('table', {'class': 'data', 'id': 'NOTES'}).text.replace(u'\xa0', ' ').split('\n')[1],
        'units': 'TAF',
        'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M'),
    }

    info.update({'AJ_posted': dt.datetime.strptime(soup.find('left').text
                                                .split('Report generated: ')[1]
                                                .rstrip(')'), 
                                                '%B %d, %Y %H:%M') })


    # ADD IN B120 DIST INFO
    url = f'http://cdec.water.ca.gov/reportapp/javareports?name=B120DIST.{report_date:%Y%m%d}'

    result = requests.get(url).content
    soup = BeautifulSoup(requests.get(url).content, 'lxml')
    tables = soup.find_all('table', {'class': 'data', 'id': 'B120DIST_1'})

    wy_list = []
    for i, table in enumerate(tables):
        if i == 0:
            for td in table.find_all('td'):
                if 'left' in str(td):
                    watershed = td.text
                    num_list = [watershed]
                    wy_list.append(num_list)
                else:
                    num_list.append(clean_td(td.text))

    # clean columns
    headers = ['Watershed','Oct thru Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Water Year Total','90%% Exceedance','10%% Exceedance','WY %% Avg']
    wy_df = pd.DataFrame(wy_list, columns=headers)

    info.update({'WY_posted': dt.datetime.strptime(tables[0].previousSibling
                                                .split('Report generated: ')[1]
                                                .rstrip(')'), 
                                                '%B %d, %Y %H:%M') })

    # difference btwn this and updated version?? https://cdec.water.ca.gov/reportapp/javareports?name=B120UP

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

