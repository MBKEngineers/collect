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
