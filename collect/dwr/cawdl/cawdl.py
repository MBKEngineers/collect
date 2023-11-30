"""
collect.dwr.cawdl.cawdl
============================================================
access CA Water Data Library surface water and well data

Stations info
https://data.cnra.ca.gov/dataset/fcba3a88-a359-4a71-a58c-6b0ff8fdc53f/resource/c2b08f48-acfd-4a5b-9799-0f3e07d83192/download/stations.csv
https://data.cnra.ca.gov/dataset/618c73fe-b28c-4399-a824-43d0278fe974/resource/03967113-1556-4100-af2c-b16a4d41b9d0/download/gwl-stations.csv
https://data.cnra.ca.gov/dataset/618c73fe-b28c-4399-a824-43d0278fe974/resource/16f256f8-35a4-4cab-ae02-399a2914c282/download/gwl-monthly.csv
https://data.cnra.ca.gov/dataset/618c73fe-b28c-4399-a824-43d0278fe974/resource/84e02633-00ca-47e8-97ec-c0093313ddcd/download/gwl-daily.csv

Continuous data links
Older (01N04E36Q001M)
https://wdlstorageaccount.blob.core.windows.net/continuous/01N04E36Q001M/por/01N04E36Q001M_Groundwater_Level_Below_Ground_Surface_Raw.csv
https://wdlstorageaccount.blob.core.windows.net/continuous/01N04E36Q001M/por/01N04E36Q001M_Groundwater_Level_Below_Ground_Surface_Daily_Mean.csv
https://wdlstorageaccount.blob.core.windows.net/continuous/01N04E36Q001M/por/01N04E36Q001M_Groundwater_Surface_Elevation_Raw.csv
https://wdlstorageaccount.blob.core.windows.net/continuous/01N04E36Q001M/por/01N04E36Q001M_Groundwater_Surface_Elevation_Daily_Mean.csv
https://wdlstorageaccount.blob.core.windows.net/continuous/01N04E36Q001M/por/01N04E36Q001M_Water_Temperature_Raw.csv
https://wdlstorageaccount.blob.core.windows.net/continuous/01N04E36Q001M/por/01N04E36Q001M_Water_Temperature_Daily_Mean.csv

More recent (B05155) Merced River at Cressey
https://wdlstorageaccount.blob.core.windows.net/continuous/B05155/por/B05155_Stage_Raw.csv
https://wdlstorageaccount.blob.core.windows.net/continuous/B05155/por/B05155_Stage_Daily_Mean.csv
https://wdlstorageaccount.blob.core.windows.net/continuous/B05155/por/B05155_Flow_Raw.csv
https://wdlstorageaccount.blob.core.windows.net/continuous/B05155/por/B05155_Flow_Daily_Mean.csv
https://wdlstorageaccount.blob.core.windows.net/continuous/B05155/por/B05155_Water_Temperature_Raw.csv
https://wdlstorageaccount.blob.core.windows.net/continuous/B05155/por/B05155_Water_Temperature_Daily_Mean.csv
https://wdlstorageaccount.blob.core.windows.net/continuous/B05155/por/B05155_Electrical_Conductivity_at_25C_Raw.csv
https://wdlstorageaccount.blob.core.windows.net/continuous/B05155/por/B05155_Electrical_Conductivity_at_25C_Daily_Mean.csv

Site Reports
https://wdlstorageaccount.blob.core.windows.net/continuousdata/docs/A0079000/POR/Site_Report.txt
https://wdl.water.ca.gov/StationDetails.aspx?Station=01N04E36Q001M
"""
# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import pandas as pd
import requests


STATION_TYPES = ['Groundwater', 'Surface Water', 'Tide Station', 'Water Quality']


def get_cawdl_site_report_url(station_number):
    """
    Arguments:
        station_number (str): string CAWDL well or station identifier
    Returns:
        url (str): the query URL for site report
    """
    url_base = 'https://wdlstorageaccount.blob.core.windows.net/continuous'
    return f'{url_base}data/docs/{station_number}/POR/Site_Report.txt'


def get_cawdl_continuous_data_url(station_number, parameter, output_interval):
    """
    Arguments:
        station_number (str): string CAWDL well or station identifier
        parameter (str): the quantity being recorded
        output_interval (str): raw or the operation like daily mean, min/max, etc.
    Returns:
        url (str): the query URL for POR continuous data
    """
    url_base = 'https://wdlstorageaccount.blob.core.windows.net/continuous'

    if parameter not in ['Air_Temperature',
                         'Chlorophyll',
                         'Dissolved_Oxygen',
                         'Dissolved_Oxygen_Percentage',
                         'Electrical_Conductivity_at_25C',
                         'Flow',
                         'Fluorescent_Dissolved_Organic_Matter',
                         'Ground_Surface_Displacement',
                         'Groundwater_Level_Below_Ground_Surface',
                         'Groundwater_Surface_Elevation',
                         'Groundwater_Temperature',
                         'pH',
                         'Salinity',
                         'Stage',
                         'Turbidity',
                         'Velocity',
                         'Water_Temperature',
                         'Water_Temperature_ADCP']:
        raise ValueError(f'ERROR: invalid `parameter`: {parameter}')

    if output_interval not in ['Raw', 'Daily_Mean', 'Daily_Min_Max']:
        raise ValueError(f'ERROR: invalid `output_interval`: {output_interval}')

    return f'{url_base}/{station_number}/por/{station_number}_{parameter}_{output_interval}.csv'


def get_cawdl_data(site_id):
    """
    download well timeseries data from CAWDL database

    Arguments:
        site_id (str): string representing CAWDL site id; ie '17202'
    Returns:
        dictionary: dictionary of 'data' and 'info' with dataframe of timeseries and station metadata
    """
    raise NotImplementedError
    cawdl_url = 'http://wdl.water.ca.gov/waterdatalibrary/groundwater/hydrographs/'
    table_url = cawdl_url + 'report_xcl_brr.cfm?CFGRIDKEY={0}&amp;type=xcl'.format(site_id)

    # read historical ground water timeseries from "recent groundwater level data" tab
    df = pd.read_csv(table_url, header=2, skiprows=[1], parse_dates=[0], index_col=0)
    return {'data': df, 'info': get_cawdl_well_detail(site_id)}


def get_cawdl_well_detail(site_id):
    """
    parse HTML file structure; extract station/well metadata

    Arguments:
        site_id (str): string representing CAWDL site id; ie '17202'
    Returns:
        dictionary: dictionary of well metadata extracted from detail page
    """
    raise NotImplementedError
    cawdl_url = 'http://wdl.water.ca.gov/waterdatalibrary/groundwater/hydrographs/'
    site_url = cawdl_url + 'brr_hydro.cfm?CFGRIDKEY={0}'.format(site_id)

    well_info = {}
    soup = BeautifulSoup(requests.get(site_url).content, 'html5lib')
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


def get_cawdl_continuous_data(site_id, parameter, output_interval):
    """
    download timeseries data from CAWDL database

    Arguments:
        site_id (str): string representing cawdl site id; ie 'B94100'
        variable (str): measurement description; ie 'STAGE' or 'FLOW' or 'CONDUCTIVITY' or 'WATER_TEMPERATURE'
        interval (str): measurement time interval; ie '15-MINUTE_DATA' or 'DAILY_MEAN' or 'DAILY_MINMAX' or
            'POINT' (default for conductivity & temp)
    Returns:
        dictionary: dictionary of 'data' and 'info' with dataframe of timeseries and station metadata
    """
    url = get_cawdl_continuous_data_url(site_id, parameter, output_interval)

    # read period of record timeseries data and rename index
    df = pd.read_csv(url, header=0, skiprows=2, parse_dates=[0], index_col=0, usecols=[0, 1, 2])
    df.index.name = 'Time and Date'

    # extract variable labels and data qualifier definitions
    record_info = pd.read_csv(url, skiprows=2, index_col=None, usecols=[3]).iloc[:, 0].dropna().tolist()
    series_info = {'Sites': record_info[1:record_info.index('Variables:')],
                   'Variables': record_info[record_info.index('Variables:') + 1:record_info.index('Qualities:')],
                   'Qualities': record_info[record_info.index('Qualities:') + 1:]}

    # return the dataframe and joined POR record qualifier info with site metadata
    return {'data': df,
            'info': {'series_metadata': series_info, **get_cawdl_continuous_data_site_report(site_id)['info']}}


def get_cawdl_continuous_data_site_report(site_id):
    """
    download site report from CAWDL database

    Arguments:
        site_id (str): string representing cawdl site id; ie 'B94100'
    Returns:
        dictionary: 'info' key with text from CAWDL site report
    """
    report_url = get_cawdl_site_report_url(site_id)

    # parse HTML file structure; extract station/well metadata
    site_info = {}
    file = requests.get(report_url)

    site_info['available series'] = []
    start_index = 9999
    end_index = 9999

    for i, line in enumerate(file):
        if i > 5: # skip title lines
            try:
                decoded_line = line.decode().strip()

                if 'Comment' in decoded_line:
                    end_index = i

                if 'Variable' in decoded_line:
                    start_index = i

                if i <= end_index:
                    if ':' in decoded_line:
                        key = decoded_line.split(':')[0]
                        value = decoded_line.split(':')[1]
                        value =  '\n'.join([s for s in value.splitlines() if s]).strip()
                        site_info.update({key: value})

                if i > start_index:
                    series = decoded_line.split('  ')[0]
                    if decoded_line != '':
                        site_info['available series'].append(series)

            except UnicodeDecodeError:
                pass

    return {'info': site_info}
