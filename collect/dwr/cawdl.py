# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import pandas
import requests


def get_cawdl_data(site_id):
    """
    Download well timeseries data from CAWDL database; return as dataframe
    ------------------|-------|-------------
    search term       | type  |  example
    ------------------|-------|-------------
        site_id       |  str  |  '17202'
    ------------------|-------|-------------
    """
    cawdl_url = 'http://wdl.water.ca.gov/waterdatalibrary/groundwater/hydrographs/'
    table_url = cawdl_url + 'report_xcl_brr.cfm?CFGRIDKEY={0}&amp;type=xcl'.format(site_id)
    site_url = cawdl_url + 'brr_hydro.cfm?CFGRIDKEY={0}'.format(site_id)

    # read historical ground water timeseries from "recent groundwater level data" tab
    df = pandas.read_csv(table_url, header=2, skiprows=[1], parse_dates=[0], index_col=0)
    # df = df.tz_localize('US/Pacific')

    # parse HTML file structure; extract station/well metadata
    well_info = {}
    soup = BeautifulSoup(requests.get(site_url).content, 'lxml')
    for table in soup.find_all('table')[1:]:
        for tr in table.find_all('tr'):
            cells = tr.find_all('td')
            if len(cells) == 2:
                key = cells[0].text.strip()
                try:
                    value = float(cells[1].text.strip())
                except ValueError:
                    value = cells[1].text.strip()
                well_info.update({key: value})

    return {'data': df, 'info': well_info}


def get_cawdl_surface_water_data(site_id, water_year, variable, interval):
    """
    Download timeseries data from CAWDL database; return as dataframe
    ------------------|-------|-------------
    search term       | type  |  example
    ------------------|-------|-------------
        site_id       |  str  |  'B94100'
    ------------------|-------|-------------
        water_year    |  int  |  2017
    ------------------|-------|-------------
        variable      |  str  |  'STAGE' or 'FLOW'
    ------------------|-------|-------------
        interval      |  str  |  '15-MINUTE_DATA' or 'DAILY MEAN' or 'DAILY MINMAX'
    ------------------|-------|-------------
    """
    cawdl_url = 'http://wdl.water.ca.gov/waterdatalibrary/docs/Hydstra/'
    table_url = cawdl_url + 'docs/{0}/{1}/{2}_{3}_DATA.CSV'.format(site_id, water_year, variable, interval)
    site_url = cawdl_url + 'index.cfm?site={0}'.format(site_id)
    report_url = cawdl_url + 'docs/{0}/POR/Site_Report.txt'.format(site_id)

    # read historical ground water timeseries from "recent groundwater level data" tab
    df = pandas.read_csv(table_url, header=[0, 1, 2], parse_dates=[0], index_col=0)
    df.index.name = ' '.join(df.columns.names)
    sensor_meta = df.columns[0]
    df.columns = [df.columns[i][2] for i in range(3)]
    meta = df['Unnamed: 3_level_2'].dropna()
    df.drop('Unnamed: 3_level_2', axis=1, inplace=True)

    # df = df.tz_localize('US/Pacific')

    # parse HTML file structure; extract station/well metadata
    site_info = {}
    soup = BeautifulSoup(requests.get(site_url).content, 'lxml')
    table = soup.find_all('table')[0]
    for tr in table.find('tbody').find_all('tr'):
        if tr.get('id') != 'my_heading_row':
            cells = tr.find_all('td')
            if len(cells) == 2:
                key = cells[0].text.strip()
                try:
                    value = float(cells[1].text.strip())
                except ValueError:
                    value = cells[1].text.strip()
                site_info.update({key: value})

    # get available data series for water_year
    site_info['available series'] = []
    for section in soup.find_all('div', {'id': 'layout_content_row'}):
        if section.find('div').get_text().strip() == str(water_year):
            for tr in section.find('table').find('tbody').find_all('tr'):
                cells = tr.find_all('td')
                site_info['available series'].append(cells[0].get_text().strip())
            break

    return {'data': df, 'info': site_info}


def get_cawdl_surface_water_site_report(site_id):
    """
    """
    url = 'http://wdl.water.ca.gov/waterdatalibrary/docs/Hydstra/docs/{}/POR/Site_Report.txt'.format(site_id)

    # parse HTML file structure; extract station/well metadata
    site_info = {}
    soup = BeautifulSoup(requests.get(site_url).content, 'lxml')
    table = soup.find('p')

    return {'info': site_info}
