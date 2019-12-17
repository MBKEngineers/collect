# -*- coding: utf-8 -*-
import datetime as dt
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


def get_wsi_data():
    """
    Water Supply Index Info
    """

    # main WRWSIHIST url
    url = 'http://cdec.water.ca.gov/reportapp/javareports?name=wsihist'

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
    return {'data': {}, 'info': {}}
