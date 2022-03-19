"""
collect.alert.alert
============================================================
query system for Sacramento County Alert website
"""
# -*- coding: utf-8 -*-
import io
import re
import unicodedata

from bs4 import BeautifulSoup, SoupStrainer
import pandas as pd
from collect import utils


SENSORS = {10: "Rain Increment", 
           11: "Rain Accumulation", 
           20: "Stage", 
           25: "Flow Volume", 
           30: "Air Temperature", 
           35: "Water Temperature", 
           40: "Wind Velocity", 
           41: "Maximum Wind Velocity", 
           42: "Avg Wind Velocity", 
           44: "Wind Direction", 
           50: "Relative Humidity", 
           53: "Barometric Pressure", 
           100: "pH", 
           124: "Signal Strength", 
           196: "Status", 
           199: "Battery"}


def _ustrip(x):
    """
    strips whitespace represented by unicode non-breaking space in additon to default white
    space stripping by python's str.strip() method
    Arguments:
        x (str): string containing an encoded whitespace
    Returns:
        cleaned (str): cleaned string
    """
    return unicodedata.normalize('NFKD', x).strip()


def get_sites(as_dataframe=True, datatype='stream'):
    """
    fetches information for all sites matching `datatype` from Sacramento County ALERT website
    
    Arguments:
        as_dataframe (bool): determines format of returned stations information.
                             If ``False`` (default), the stations will be a dict with 
                             station ids as keys mapped to a dict of station variables. 
                             If ``True`` then the result will be a pandas.DataFrame 
                             object containing the equivalent information.
        datatype (str): one of 'stream', 'rain', or 'any'
    Returns:
        result (pandas.DataFrame or dict): a dictionary containing station information
    """
    if datatype == 'any':
        return get_sites_from_list(as_dataframe=as_dataframe, sensor_class=None)

    measure = {'rain': 'rain', 'stream': 'level'}.get(datatype)
    group_type_id = {'rain': 14, 'stream': 19, 'temperature': 30}.get(datatype)

    url = f'https://www.sacflood.org/{measure}?&view_id=1&group_type_id={group_type_id}'
    soup = BeautifulSoup(utils.get_session_response(url).text, 'lxml')
    df = pd.read_html(str(soup.find('table')))[0]

    # strip whitespace from columns
    df.columns = [_ustrip(x) for x in df.columns]

    if as_dataframe:
        return df
    return df.to_dict()


def get_sites_from_list(as_dataframe=True, sensor_class=None):
    """
    returns URLs and site IDs for all Sac Alert sites included in main list view

    Arguments:
        as_dataframe (bool): flag for specifying result as a dataframe
    Returns:
        result (pandas.DataFrame or dict): collection of site names, URLs and IDs, as dataframe or dictionary
    """
    url = 'https://www.sacflood.org/list/'
    if sensor_class:
        url += '?&sensor_class={}'.format(sensor_class)
    soup = BeautifulSoup(utils.get_session_response(url).text, 'lxml')

    entries = []
    for x in soup.find_all('a', {'class': None, 'target': None}, 
                           href=lambda href: href and href.startswith('/site/?site_id=')):
        href = x.get('href')
        site_id, site_slug = re.findall(r'site_id=(\d+)&site=(.*)', href)[0]
        entries.append({'title': x.text, 
                        'site_id': site_id, 
                        'site_slug': site_slug, 
                        'href': 'https://www.sacflood.org' + href})

    df = pd.DataFrame(entries)
    if as_dataframe:
        return df
    return df.to_dict()


def get_site_notes(site_id):
    """
    Arguments:
        site_id (int): the numeric site identifier
    Returns:
        (dict): site metadata from page notes
    """
    url = f'https://www.sacflood.org/site/?site_id={site_id}'
    strainer = SoupStrainer('div', {'class': 'card-body'})
    soup = BeautifulSoup(utils.get_session_response(url).text, 'lxml', parse_only=strainer)
    for card in soup.find_all('div', {'class': 'card-body'}):
        if 'Notes' in card.find('h3', {'class': 'card-title'}).text:
            notes_block = card.find('p', {'class': 'list-group-item-text'})
            result = {'site_id': site_id, 
                      'url': url, 
                      'comment': _ustrip(notes_block.find('em').text)}
            for x in notes_block.find_all('strong'):
                if x.text.endswith(':'):
                    result[_ustrip(x.text)] = _ustrip(x.nextSibling)
            return result
    return {}


def get_site_location(site_id):
    """
    Arguments:
        site_id (int): the numeric site identifier
    Returns:
        (dict): site location metadata from page notes
    """
    url = f'https://www.sacflood.org/site/?site_id={site_id}'
    result = {'site_id': site_id, 'url': url}
    soup = BeautifulSoup(utils.get_session_response(url).text, 'lxml')
    cards = soup.find_all('div', {'class': 'card-body'})
    for card in cards:
        if 'Map' in card.find('h3', {'class': 'card-title'}).text:
            block = card.find('div', {'class': 'box border-top'})
            result['latitude'], result['longitude'] = map(float, block.find('a').text.split(','))
            return result
    return result


def get_site_sensors(site_id):
    """
    Arguments:
        site_id (int): the numeric site identifier
    Returns:
        (dict): site list of sensors from page
    """
    url = f'https://www.sacflood.org/site/?site_id={site_id}'
    result = {'site_id': site_id, 'url': url, 'sensors': []}
    soup = BeautifulSoup(utils.get_session_response(url).text, 'lxml')
    cards = soup.find_all('div', {'class': 'card-body'})
    for card in cards:
        if 'Sensors' in card.find('h3', {'class': 'card-title'}).text:
            block = card.find('ul', {'class': 'list-group list-group-flush'})
            for row in block.find_all('div', {'class': 'row'}):
                a = row.find('a', href=True)
                measure = list(row.find('div', {'class': 'col-lg-2 col-6 data'})
                                  .find('h4', {'class' :'list-group-item-heading'})
                                  .stripped_strings)[0].split()[-1]
                result['sensors'].append({'title': a.text, 
                                          'href': a.get('href'), 
                                          'units': measure,
                                          'device_id': int(a.get('href').split('device_id=')[-1].split('&')[0])})
            return result
    return result


def get_query_url(site_id, device_id, start, end):
    """
    build CSV export query URL for Sacramento County ALERT stream sensor to download data specified site and device

    Arguments:
        start (datetime.datetime): starting datetime for download of data
        end (datetime.datetime): ending datetime for download of data
        site_sensor (str): Site & Sensor id's combined with '-' to identify site and 
                              sensor from website (e.g. Site = 'Alpine/Unionhouse'; 
                              Sensor = 'Unionhouse Creek'; site_sensor='Alpine/Unionhouse-Unionhouse Creek')
        datatype (str): 
    Returns:
        url (str): query url
    """
    query_base = 'https://www.sacflood.org/export/file/?'
    query_params = [f'site_id={site_id}', # 1137
                    # f'site={site_slug}', # 5c954985-4f03-464d-a218-623263dad34f
                    f'device_id={device_id}', # 5
                    # f'device={device_slug}', #fc020a30-dd47-4f3e-a008-1cc72c0398f3
                    'mode=',
                    'hours=',
                    f'data_start={start:%Y-%m-%d%%20%H:%M:%S}',
                    f'data_end={end:%Y-%m-%d%%20%H:%M:%S}',
                    'tz=US%2FPacific',
                    'format_datetime=%25Y-%25m-%25d+%25H%3A%25i%3A%25S',
                    'mime=txt',
                    'delimiter=comma']
    return query_base + '&'.join(query_params)


def get_device_series(site_id, device_id, start, end, ascending=True):
    url = get_query_url(site_id, device_id, start, end)
    response = io.StringIO(utils.get_session_response(url).text)
    df = pd.read_csv(response)
    return df.sort_values(by='Reading', ascending=ascending)


def get_data(site_id, start, end, device_ids=None, ascending=True, as_dataframe=True):
    """
    retrieves Sacramento County ALERT site timeseries data; defaults to providing data for all
    available sensors/devices for the given site

    Arguments:
        site_id (str): the integer site ID
        start (datetime.datetime): desired timeseries start date/time
        end (datetime.datetime): desired timeseries end date/time
        device_ids (int): optional list of device_ids for filtering site's timeseries
        ascending (bool): flag to return data in ascending order of Reading date/time
        as_dataframe (bool): If True then 'values' key of the returned dictionary will be a Pandas DataFrame
    Returns:
        result (dict): dictionary of station notes and sensor readings
    """
    # check that start/end times are in correct order
    if end <= start:
        raise NotImplementedError('Specified start must be < end.')

    # get site information like all valid devices for site
    meta = get_site_sensors(site_id)
    
    # loop through available measurement devices
    frames = []
    for device in meta['sensors']:
        if device_ids is not None:
            if device['device_id'] not in device_ids:
                continue

        # query timseries data
        site_url = get_query_url(site_id, device['device_id'], start, end)
        df = get_device_series(site_id, device['device_id'], start, end, ascending=ascending)
        
        # add unique site and device ID combination
        df.loc[:, 'Site ID'] = meta['site_id']
        df.loc[:, 'Device Name'] = device['title']
        df.loc[:, 'Device ID'] = device['device_id']
        frames.append(df)
    
    # combine all device timeseries dataframes
    df = pd.concat(frames, axis=0)

    # return site timeseries and metadata
    return {'notes': get_site_notes(site_id),
            'data': df if as_dataframe else df.to_dict()}
