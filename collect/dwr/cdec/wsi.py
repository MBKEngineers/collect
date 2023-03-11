"""
collect.dwr.wsi
============================================================
access DWR water supply index data
"""
# -*- coding: utf-8 -*-
import datetime as dt
import io
from bs4 import BeautifulSoup
import pandas as pd
import requests
import re
from io import StringIO


def clean_fwf_df(table_text, col_spec, header, skiprows=[]):
    """
    write text representing table to string buffer and parse as fixed-width
    file; write results to
    """
    text_buffer = StringIO()
    text_buffer.write(table_text)
    text_buffer.seek(0)
    return pd.read_fwf(text_buffer, 
                       colspecs=col_spec, 
                       index_col=0, 
                       header=header, 
                       skiprows=skiprows).dropna()


def _parse_reconstructed_wyi_table(table_text):
    text_buffer = StringIO(wyi_table)
    text_buffer.write(table_text)
    text_buffer.seek(0)

    df = pd.read_fwf(text_buffer, header=[0], skiprows=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12], index_col=0)

    column_headers = {
        'Oct-Mar': ('Runoff (maf)', 'Oct-Mar'),
        'Apr-Jul': ('Runoff (maf)', 'Apr-Jul'),
        'WYsum': ('Runoff (maf)', 'WYsum'),
        'Index': ('WY Index', 'Index'),
        'Yr-type': ('WY Index', 'Yr-type')
    }

    df.columns = pd.MultiIndex.from_tuples(
        [('Sacramento Valley', *column_headers.get(c)) for c in df.columns[:5]] 
        + [('San Joaquin Valley', *column_headers.get(c.rstrip('.1'))) for c in df.columns[5:]]
    )

    return df




def get_wsi_data():
    """
    Water Supply Index Info
    """

    # main WRWSIHIST url
    url = 'https://cdec.water.ca.gov/reportapp/javareports?name=wsihist'

    # parse HTML file structure; AJ forecast table
    soup = BeautifulSoup(requests.get(url).content, 'html5lib')
    table = soup.find('pre').text

    # three tables on this page
    wyi_table, eight_river_runoff_table, official_year_class_table = table.strip().rstrip('.END').strip().split('\n\n\n')
    official_year_class_table, abbreviations_and_notes = official_year_class_table.split('Abbreviations')
    abbreviations_and_notes = 'Abbreviations' + abbreviations_and_notes

    # parse content of reconstructed Sac and SJ Valley Water Year Hydrologic Classification Indices
    wyi_data = clean_fwf_df(wyi_table, 
                            [(0, 4), (5, 12), (13, 20), (21, 28), (29, 36), (36, 44), (45, 52), (53, 60), (61, 69), (70, 76), (76, 85)], 
                            header=12, 
                            skiprows=[13])

    # parse content of 8-river runoff indices
    eight_river_data = clean_fwf_df(eight_river_runoff_table, 
                                    [(0, 4), (5, 12), (13, 20), (21, 28), (30, 37), (40, 47), (48, 60)], 
                                    header=3, 
                                    skiprows=[4])

    # parse content of Official Year Classifications based on May 1 Runoff Forecasts
    official_year_data = clean_fwf_df(official_year_class_table, 
                                      [(0, 4), (5, 20), (20, 30), (31, 52), (52, 70)], 
                                      header=3)

    # metadata
    info = {
        'url': url,
        'type': 'WRWSIHIST',
        # 'title': '', 
        # 'caption': '', 
        # 'notes': '',
        'units': 'MAF',
        'downloaded': dt.datetime.now().strftime('%Y-%m-%d %H:%M')
    }

    return {'data': {'wyi': wyi_data, '8-river': eight_river_data, 'official': official_year_data}, 'info': info}


def get_wsi_forecast():
    """
    http://cdec.water.ca.gov/reportapp/javareports?name=wsi
    """

    url = 'http://cdec.water.ca.gov/reportapp/javareports?name=wsi'

    result = requests.get(url).content
    result = BeautifulSoup(result, 'lxml').find('pre').text

    data = {}

    title = re.findall(r'\d{4} Water Year Forecast as of \w+ \d{1,2}, \d{4}', result)
    index_line = re.findall(r'\n.*\n--+\n.*\n', result, re.MULTILINE)
    index_dict = {'SRR': index_line[0], 'SVI': index_line[1], 'SJI': index_line[2]}

    current_runoff, prev_runoff = re.findall(r'\d{4} \(\w+ \w+\) \= +(.*) MAF +(.*)', result)

    # data = pd.DataFrame(columns=['Index name','99%','90%','75%','50%','25%','10%'])
    # data = pd.DataFrame()
    for index, line in index_dict.items():
        nums = re.findall(r'\d{1,3}\.\d', line)
        percentages = re.findall(r'\(\d{1,3}\%\)', line)

        data.update({index: {'99%': nums[0], '90%': nums[1], '75%': nums[2], '50%': nums[3], '25%': nums[4], '10%': nums[5]}})

        # buf = io.StringIO()
        # buf.write(line)
        # buf.seek(0)
        # df = pd.read_fwf(buf, header=[0], skiprows=[0], index_col=None).drop([0])
        # print(df.columns)
        # df.columns = ['Forecast Date','99%','90%','75%','50%','25%','10%']

    info = {'title': title, 'Water Year Runoff through end of last month': {'Current year': current_runoff, 'Previous year': prev_runoff}}

    return {'info': info, 'data': data}
