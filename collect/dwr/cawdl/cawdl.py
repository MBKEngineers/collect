"""
collect.dwr.cawdl.cawdl
============================================================
access CA Water Data Library surface water and well data
"""
# -*- coding: utf-8 -*-
import datetime as dt
import io
from bs4 import BeautifulSoup
import pandas as pd
from collect.utils import utils


def get_cawdl_site_detail_url(site_id):
    """
    get the URL for the station detail page with links to timeseries charts and data downloads

    Arguments:
        site_id (str): string representing CAWDL well or station identifier; ie '01N04E36Q001M', 'B05155'
    Returns:
        url (str): the URL for site detail page
    """
    return f'https://wdl.water.ca.gov/StationDetails.aspx?Station={site_id}'


def get_cawdl_site_report_url(site_id):
    """
    get the URL for the station site report, which is a text-format detail page

    Arguments:
        site_id (str): string representing CAWDL well or station identifier; ie '01N04E36Q001M', 'B05155'
    Returns:
        url (str): the query URL for site report
    """
    url_base = 'https://wdlstorageaccount.blob.core.windows.net/continuous'
    return f'{url_base}data/docs/{site_id}/POR/Site_Report.txt'


def get_cawdl_continuous_data_url(site_id, parameter, output_interval):
    """
    get the URL for the station period of record, continuous data timeseries for a parameter and output interval

    Arguments:
        site_id (str): string representing CAWDL well or station identifier; ie '01N04E36Q001M', 'B05155'
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

    return f'{url_base}/{site_id}/por/{site_id}_{parameter}_{output_interval}.csv'


def get_cawdl_site_detail(site_id):
    """
    parse HTML file structure; extract station/well metadata for continuous data sites

    Arguments:
        site_id (str): string representing CAWDL well or station identifier; ie '01N04E36Q001M', 'B05155'
    Returns:
        info (dict): dictionary of site info extracted from detail page
    """
    url = get_cawdl_site_detail_url(site_id)
    soup = BeautifulSoup(utils.get_session_response(url).content, 'html5lib')

    # parse the station detail table
    station_table = soup.find_all('table', {'class': 'OSFillParent'})[0]
    station_df = pd.read_html(f'<div>{str(station_table)}</div>', flavor='bs4', index_col=0)[0]
    station_df.index = station_df.index.str.rstrip(' :')
    info = station_df[1].to_dict()

    # parse the data availability table
    data_table = soup.find_all('table', {'class': 'OSFillParent'})[1]
    data_df = pd.read_html(f'<div>{str(data_table)}</div>', flavor='bs4')[0]
    data_df['Parameter'] = data_df['Parameter'].fillna(method='ffill')

    # add data availability table and site report URL to site info dictionary
    info.update({'url': url,
                 'availability': data_df,
                 'report': get_cawdl_site_report_url(site_id)})

    # return the site data as a dictionary
    return info


def get_cawdl_continuous_data(site_id, parameter, output_interval, start=None, end=None):
    """
    download timeseries data from CAWDL database with optional start and end filter

    Arguments:
        site_id (str): string representing CAWDL well or station identifier; ie '01N04E36Q001M', 'B05155'
        variable (str): measurement description; ie 'STAGE' or 'FLOW' or 'CONDUCTIVITY' or 'WATER_TEMPERATURE'
        interval (str): measurement time interval; ie '15-MINUTE_DATA' or 'DAILY_MEAN' or 'DAILY_MINMAX' or
            'POINT' (default for conductivity & temp)
        start (datetime.datetime): optional start time to filter timeseries data record
        end (datetime.datetime): optional start time to filter timeseries data record
    Returns:
        dictionary: dictionary of 'data' and 'info' with dataframe of timeseries and station metadata
    """
    url = get_cawdl_continuous_data_url(site_id, parameter, output_interval)

    # read period of record timeseries data and rename index
    df = pd.read_csv(url, header=0, skiprows=2, parse_dates=[0], index_col=0, usecols=[0, 1, 2])
    df.index.name = 'Time and Date'

    # time window is specified
    if isinstance(start, dt.datetime):
        df = df.truncate(before=start)

    if isinstance(end, dt.datetime):
        df = df.truncate(after=end)

    # extract variable labels and data qualifier definitions
    record_info = pd.read_csv(url, skiprows=2, index_col=None, usecols=[3]).iloc[:, 0].dropna().tolist()
    series_info = {'Sites': record_info[1:record_info.index('Variables:')],
                   'Variables': record_info[record_info.index('Variables:') + 1:record_info.index('Qualities:')],
                   'Qualities': record_info[record_info.index('Qualities:') + 1:]}

    # return the dataframe and joined POR record qualifier info with site metadata
    return {'data': df,
            'info': {'series_metadata': series_info, **get_cawdl_continuous_data_site_report(site_id)['info']}}


def get_cawdl_continuous_data_site_report(site_id, include_raw=False):
    """
    download CAWDL database site report for continuous data stations by site_id

    Arguments:
        site_id (str): string representing CAWDL well or station identifier; ie '01N04E36Q001M', 'B05155'
        include_raw (bool): flag for whether to include the raw site report text
    Returns:
        dictionary: 'info' key with text from CAWDL site report
    """
    url = get_cawdl_site_report_url(site_id)

    # metadata dictionary
    info = {'url': url}

    # parse HTML file structure; extract station/well metadata
    response = utils.get_session_response(url).text
    lines = response.split('\r\n')

    # add the raw page text if specified
    if include_raw:
        info.update({'raw': response})

    # break on these section titles to separate text-formatted tables
    section_titles = ['DATUM SHIFTS',
                      'GAUGINGS',
                      'PERIOD OF RECORD (ARCHIVE)',
                      'RATING TABLES',
                      'SITE DESCRIPTION',
                      'STATION DESCRIPTION',
                      'TIME SHIFTS',
                      'TIME-BASED TABLES']

    # separate site report into sections for processing
    sections = {}
    header = None
    for line in lines:
        clean = line.strip()
        if clean.endswith(' Page 1'):
            info['published'] = dt.datetime.strptime(clean.split('Output')[1].strip(), '%m/%d/%Y Page 1').date()
        if clean in section_titles and clean not in sections:
            header = clean
            sections.update({header: []})
        elif bool(clean) and header is not None:
            if (clean.startswith('California Department of Water Resources')
                    or clean.startswith('HYSITREP')
                    or clean.startswith(site_id)):
                continue
            sections[header].append(line)

    def _parse_description(lines):
        ret = {}
        for line in lines:
            key, value = line.split(':', 1)
            ret[key] = value.strip()
        return ret

    def _parse_table(lines, **kwargs):
        return pd.read_fwf(io.StringIO('\n'.join(lines)), **kwargs)

    if 'SITE DESCRIPTION' in sections:
        info.update({'site_description': _parse_description(sections['SITE DESCRIPTION'])})

    if 'STATION DESCRIPTION' in sections:
        info.update({'station_description': _parse_description(sections['STATION DESCRIPTION'])})

    if 'DATUM SHIFTS' in sections:
        info.update({'datum_shifts': _parse_table(sections['DATUM SHIFTS'])})

    if 'GAUGINGS' in sections:
        info.update({'gaugings': [x.strip() for x in sections['GAUGINGS']]})

    if 'PERIOD OF RECORD (ARCHIVE)' in sections:
        info.update({'period_of_record_archive': _parse_table(sections['PERIOD OF RECORD (ARCHIVE)'],
                                                              header=0,
                                                              colspecs=[(0, 40), (40, 56), (56, 100)])})

    if 'RATING TABLES' in sections:
        colspecs = [(0, 32), (32, 60), (60, 71), (71, 80), (80, 100), (100, 200)]
        info.update({'rating_tables': _parse_table(sections['RATING TABLES'],
                                                   header=0,
                                                   skiprows=1,
                                                   index_col=None,
                                                   colspecs=colspecs)})

    if 'TIME SHIFTS' in sections:
        info.update({'time_shifts': _parse_table(sections['TIME SHIFTS'],
                                                 skiprows=1,
                                                 names=['Date', 'Time', 'Shift', 'Comment'])})

    if 'TIME-BASED TABLES' in sections:
        colspecs = [(0, 40), (40, 50), (50, 59), (59, 76), (76, 88), (88, 100)]
        info.update({'time_based_tables': _parse_table(sections['TIME-BASED TABLES'], header=0, colspecs=colspecs)})

    return {'info': info}


def get_cawdl_dataset_overview(dataset):
    """
    station info provided in summary tables on the CNRA open data portal

    Arguments:
        dataset (str): the name of the CSV file
    Returns:
        df (pandas.DataFrame): dataframe with contents of specified CSV dataset
    """
    if dataset not in ['period_of_record',
                       'gwl-stations',
                       'gwl-quality_codes',
                       'gwl-monthly',
                       'gwl-daily',
                       'stations',
                       'station-trace-download-links']:
        raise ValueError(f'ERROR: `dataset` {dataset} is not available.')

    resource_path = {
        'period_of_record': '3f96977e-2597-4baa-8c9b-c433cea0685e/resource/8ff3a841-d843-405a-a360-30c740cc8691',
        'gwl-stations': '618c73fe-b28c-4399-a824-43d0278fe974/resource/03967113-1556-4100-af2c-b16a4d41b9d0',
        'gwl-quality_codes': '618c73fe-b28c-4399-a824-43d0278fe974/resource/06437a09-ac72-4d5b-91a7-e5963349b486',
        'gwl-monthly': '618c73fe-b28c-4399-a824-43d0278fe974/resource/16f256f8-35a4-4cab-ae02-399a2914c282',
        'gwl-daily': '618c73fe-b28c-4399-a824-43d0278fe974/resource/84e02633-00ca-47e8-97ec-c0093313ddcd',
        'stations': 'fcba3a88-a359-4a71-a58c-6b0ff8fdc53f/resource/c2b08f48-acfd-4a5b-9799-0f3e07d83192',
        'station-trace-download-links': 'fcba3a88-a359-4a71-a58c-6b0ff8fdc53f/resource/cdb5dd35-c344-4969-8ab2-d0e2d6c00821',
    }.get(dataset)

    return pd.read_csv(f'https://data.cnra.ca.gov/dataset/{resource_path}/download/{dataset}.csv')
