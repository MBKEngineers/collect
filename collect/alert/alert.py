"""
collect.alert.alert
============================================================
Sacramento County Alert system
"""
# -*- coding: utf-8 -*-
import datetime as dt
import os
import re
import string

from bs4 import BeautifulSoup, SoupStrainer
import pandas as pd
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


def _get_session_response(url):
    session = requests.Session()
    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session.get(url, verify=False)


def _check_for_no_data(url): 
    soup = BeautifulSoup(_get_session_response(url).text, 
                         'lxml', 
                         parse_only=SoupStrainer('div', {'class': 'app-alert fade in alert alert-info'}))
    return 'No data' not in soup.text


def get_stations(as_dataframe=True, datatype='stream'):
    """
    fetches stream stage stations information from Sacramento County ALERT website
    
    Arguments:
        as_dataframe (bool): This determines what format the stations information is returned as. If ``False`` (default), the stations will be a dict with station ids as keys mapped to a dict of station variables. If ``True`` then the entire sites_dict will be a pandas.DataFrame object containing the equivalent information.
        datatype (str): either 'stream' or 'rain' for stream flow data or rain gage data, respectively
    Returns:
        sites_dict (dict): a dictionary containing station information
    """
    measure = {'rain': 'rain', 'stream': 'level'}.get(datatype)
    group_type_id = {'rain': 14, 'stream': 19}.get(datatype)

    url = f'https://www.sacflood.org/{measure}?&view_id=1&group_type_id={group_type_id}&grouping_id='
    # https://www.sacflood.org/list/
    # https://www.sacflood.org/rain/
    # https://www.sacflood.org/level/

    soup = BeautifulSoup(_get_session_response(url).text, 'lxml')
    df = pd.read_html(str(soup.find('table')))[0]

    # strip whitespace from columns
    df.columns = [x.strip() for x in df.columns]
    for x in df.columns:
        first_entry = df[x][0]
        if isinstance(first_entry, str):
            if not all(x in string.printable for x in first_entry):
                df[x] = df[x].apply(lambda x: x[:-1])

    if as_dataframe:
        return df
    return df.to_dict()


def download_station_data(start_datetime, end_datetime, url, datatype, ascending=False):
    if _check_for_no_data(url):

        if 'soup_next' in locals():
            del soup_next
        web_text = _get_session_response(url).text
        soup_all = BeautifulSoup(web_text, 'lxml')
        soup = BeautifulSoup(web_text, 'lxml', parse_only=SoupStrainer('h4'))
        next_page=True
        page_number = 2
        base_url = 'http://www.sacflood.org'
        if datatype == 'stream':
            data_df = pd.DataFrame(columns=['stage [feet]'])
        else:
            data_df = pd.DataFrame(columns=['incremental precip [inches]'])
        while next_page == True:
            if 'soup_next' in locals():
                soup_all = soup_all_next
                soup = soup_next

            for hhh, h4 in enumerate(soup):
                # import pdb;pdb.set_trace()
                if hhh == 0:
                    try:                        
                        dt_text = h4.text.split('ft')[1].strip()
                        record_dt = dt.datetime.strptime(dt_text, '%Y-%m-%d %H:%M:%S')
                    except:
                        pass
                elif hhh % 2 == 1:   #if odd
                    try:
                        stage_reading = float(h4.text.split()[0].strip())
                        dat = h4.text.split()[2]
                        tim = h4.text.split()[3]
                        record_dt = dt.datetime.strptime('{0} {1}'.format(dat, tim), '%Y-%m-%d %H:%M:%S')
                        data_df.loc[record_dt] = stage_reading
                    except:
                        stage_reading = float(h4.text.split()[0].strip())
                        if record_dt in data_df.index:
                            pass
                        else:
                            data_df.loc[record_dt] = stage_reading

                else:
                    try:
                        stage_reading = float(h4.text.split('ft')[0].strip())
                        data_df.loc[record_dt,'stage_ft'] = stage_reading
                    except:
                        dat = h4.text.split()[0].strip()
                        tim = h4.text.split()[1].strip()
                        record_dt = dt.datetime.strptime('{0} {1}'.format(dat, tim), '%Y-%m-%d %H:%M:%S')
            try:
                if page_number == 2:
                    elm = soup_all.find('a', {'title':'Next'})
                    elm_link = elm['href']
                    elm_replace = elm['href']
                else:
                    elm_replace = elm_link.replace('page=2', f'page={page_number:.0f}')
                    elm = elm_link.split('&')[0]
                next_page_link = base_url + elm_replace
                next_page_exists = _check_for_no_data(next_page_link)
                if next_page_exists is False:
                    next_page = False

                next_text = _get_session_response(next_page_link).text
                soup_next = BeautifulSoup(next_text, 'lxml', parse_only=SoupStrainer('h4'))
                soup_all_next = BeautifulSoup(next_text, 'lxml')
            except:
                pass
            page_number += 1
            if record_dt - start_datetime < dt.timedelta(0, 3600):
                next_page = False
            try:
                if record_dt == last_record_dt:
                    next_page = False
            except:
                pass
            last_record_dt = record_dt
        if ascending:
            data_df.sort_values(['stage_ft'], ascending=True)
        return data_df
    


    else:
        raise NotImplementedError('Data does not exist for url:\n{0}'.format(url))    


def build_sacco_alert_url(start_datetime, end_datetime, site_sensor, datatype='stream'):
    """
    Sacramento County ALERT stream sensors urls to download data

    Arguments:
        start_datetime (datetime.datetime): starting datetime for download of data
        end_datetime (datetime.datetime): ending datetime for download of data
        station_sensor (str): Site & Sensor id's combined with '-' to identify site and 
                              sensor from website (e.g. Site = 'Alpine/Unionhouse'; 
                              Sensor = 'Unionhouse Creek'; site_sensor='Alpine/Unionhouse-Unionhouse Creek')
    Returns:
        url (str): query url
    """
    site_sensor = site_sensor.split('-')[0] if datatype == 'precipitation' else site_sensor
    site_attr = get_site_url_attr_from_map(datatype)[site_sensor]
    query_params = ['time_zone=US%2FPacific', 
                    'site_id={0}'.format(site_attr['site_id']), 
                    'site={0}'.format(site_attr['site']), 
                    'device_id={0}'.format(site_attr['device_id']), 
                    'device={0}'.format(site_attr['device']), 
                    'bin=86400', 
                    'range=Custom+Range'
                    'legend=true',
                    'thresholds=true',
                    'refresh=',
                    'show_raw=true',
                    'show_quality=true',
                    'data_start={0:%Y-%m-%d+%H%%3A%M%%3A%S}'.format(start_datetime), # use double % to escape the percent character
                    'data_end={0:%Y-%m-%d+23%%3A59%%3A59}'.format(end_datetime)] # use double % to escape the percent character
    url = 'http://www.sacflood.org/sensor?' + '&'.join(query_params)
    return url


def split_date_range_increments_lt_1year(start_datetime, end_datetime):
    data_years = (end_datetime - start_datetime).days / 365
    dates_list = []
    end_dt = end_datetime
    st_dt = start_datetime
    another_page = True
    while another_page == True: 
        new_start = end_dt - dt.timedelta(364)
        if new_start > start_datetime:
            dates_list.append([new_start, end_dt])
        else: 
            dates_list.append([st_dt, end_dt])
            another_page = False
        next_end = end_dt - dt.timedelta(365)
        end_dt = next_end
    return dates_list


def get_lat_lon():

    result = {}
    url = 'https://www.sacflood.org/home'
    
    soup = BeautifulSoup(_get_session_response(url).text, 'lxml')

    counter = 0
    system_wide_link_endings = []
    for link in soup.find_all('a', href=True):
        if link.next_element == 'System Wide':
            counter += 1
            if counter <= 2:
                system_wide_link_endings.append(link['href'])

    for datatype in ['stream', 'precipitation']:

        for link_end in system_wide_link_endings:
            actual_link_end = link_end.split('..')[-1]
            
            for station in get_stations(as_dataframe=True, datatype=datatype)['Site'].values:

                if 'Timeout' in station:
                    station = station.split('Timeout')[0].strip()

                for link in BeautifulSoup(_get_session_response('/'.join([url, actual_link_end])).text,
                                              'lxml').find_all('a', href=True):

                    if link.next_element == station:
                        try:
                            station_text = _get_session_response('/'.join([url, link['href']])).text
                            pattern = '<strong>Latitude: </strong>(.*)<strong>Longitude: </strong>(.*)</a></span>'
                            pattern2 = '<title>Sac County ALERT System Site:{0}\((.*)\)</title>'.format(station.replace('(', '\(').replace(')', '\)') if '(' in station else station)
                            lat_lon = re.findall(pattern, station_text)
                            lat = lat_lon[0][0].strip()
                            lon = lat_lon[0][1].strip()
                            result[station] = {
                                'lat': lat,
                                'lon': lon,
                                'id_number': re.findall(pattern2, station_text)[0]
                            }

                        except:
                            print('Station {0} does NOT contain LAT or LON'.format(station))
    return result


def get_site_url_attr_from_map(datatype):
    """
    Arguments:
        datatype (str): one of ['stream', 'precipitation']
    Returns:
        result (dict): a dictionary storing the station attributes for all stations of specified datatype
    """
    sensor_class = {'stream': 20, 'precipitation': 10}.get(datatype)
    mode = {'stream': 'sensor', 'precipitation': 'accumulation'}.get(datatype)
    interval = {'stream': '', 'precipitation': 1440}.get(datatype)

    if datatype == 'stream':
        pattern = '\<a href="(.*)"\>\<img src=".*" width="13" height="13"  alt=""data-toggle="tooltip" title="\<p\>&lt;strong&gt;(.*)&lt;/strong&gt; &lt;small&gt;\((.*)\)&lt;/small&gt;&lt;br/&gt;&lt;strong&gt;(.*)&lt;/strong&gt; &lt;small&gt;\('
    elif datatype == 'precipitation':        
        pattern = '\<a href="(.*)"\>\<img.*src=".*" width="13" height="13"  alt=""data-toggle="tooltip" title="\<p\>&lt;strong&gt;(.*)&lt;/strong&gt; &lt;small&gt;\((.*)\)&lt;/small&gt;&lt;br/&gt;.*&lt;br/&gt;(.*)\</p\>" /\>\<span class'
    else:
        raise NotImplementedError('datatype must be one of "stream" or "precipitation"')

    url = f'https://www.sacflood.org/map?&map_id=2&view_id=1&sensor_class={sensor_class}&mode={mode}&interval={interval}'

    # initialize empty dict for response
    result = {}

    for site_attr in re.findall(pattern, _get_session_response(url).text):
        site_url_end = site_attr[0]
        site_name = site_attr[1]
        site_numb = site_attr[2]
        sensor_name = site_attr[3]
        site_sensor_name = '{0}-{1}'.format(site_name, sensor_name)        
        result[site_sensor_name if datatype == 'stream' else site_name] = {}

        for attr in site_url_end.split('?')[-1].split('&amp;'):
            name = attr.split('=')[0]
            value = attr.split('=')[-1]
            result[site_sensor_name if datatype == 'stream' else site_name][name] = value

    return result


def get_station_notes(url, datatype):
    notes_dict = {}
    web_text = _get_session_response(url).text

    if datatype == 'stream':
        soup = BeautifulSoup(web_text, 
                             'lxml', 
                             parse_only=SoupStrainer('p', {'class': 'list-group-item-text'}))
        for x in soup.findAll('strong'):
            header = x.text
            header_note_raw = x.nextSibling
            header_note = header_note_raw.split('&nbsp;')[-1]
            notes_dict[header] = header_note
    else:
        soup_all = BeautifulSoup(web_text, 'lxml')
        notes_df = pd.read_html(str(soup_all.find('table')), header=1, index_col=0)[0]
        notes_dict['precip_freq_est_inches'] = notes_df

    return notes_dict


def get_site_notes(site_id):
    """
    Arguments:
        site_id (int): the numeric site identifier
    Returns:
        (dict): site metadata from page notes
    """
    url = f'https://www.sacflood.org/site/?site_id={site_id}'
    soup = BeautifulSoup(_get_session_response(url).text, 'lxml')
    cards = soup.find_all('div', {'class': 'card-body'})
    for card in cards:
        if 'Notes' in card.find('h3', {'class': 'card-title'}).text:
            notes_block = card.find('p', {'class': 'list-group-item-text'})
            result = {'site_id': site_id, 
                      'url': url, 
                      'comment': notes_block.find('em').text}
            elements = list(notes_block.stripped_strings)
            for i, x in enumerate(elements):
                if x.endswith(':'):
                    result.update({x: elements[i+1]})
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
    soup = BeautifulSoup(_get_session_response(url).text, 'lxml')
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
    soup = BeautifulSoup(_get_session_response(url).text, 'lxml')
    cards = soup.find_all('div', {'class': 'card-body'})
    for card in cards:
        if 'Sensors' in card.find('h3', {'class': 'card-title'}).text:
            block = card.find('ul', {'class': 'list-group list-group-flush'})
            for row in block.find_all('div', {'class': 'row'}):
                a = row.find('a', href=True)
                measure = list(row.find('div', {'class': 'col-lg-2 col-6 data'}).find('h4', {'class' :'list-group-item-heading'}).stripped_strings)[0].split()[-1]
                result['sensors'].append({'title': a.text, 
                                          'href': a.get('href'), 
                                          'units': measure,
                                          'device_id': a.get('href').split('device_id=')[-1].split('&')[0]})
            return result
    return result


def get_station_data(start_datetime, end_datetime, site_sensor, ascending=False, as_dataframe=True, datatype='stream'):
    """
    retrieves Sacramento County ALERT stream sensor data
    
    Arguments:
        start_datetime (datetime.datetime): Date of start of information desired.  This will start at midnight if time is not otherwise set
        end_datetime (datetime.datetime): Date of end of information desired.  This will end the minute before midnight if time is nototherwise set.
        station_sensor (str): Site & Sensor id's combined with '-' to identify site and sensor from website (e.g. Site = 'Alpine/Unionhouse'; Sensor = 'Unionhouse Creek';  site_sensor='Alpine/Unionhouse-Unionhouse Creek')
        ascending (bool): If True then data ordered from start_date to end_date; if False (default) then data ordered from end date to start date.
        as_dataframe (bool): If True then 'values' key of the returned dictionary will be a Pandas DataFrame
    Returns:
        result (dict): dictionary of station notes and sensor readings
    """
    datatype = {'streamgage': 'stream', 'precip': 'precipitation'}.get(datatype, datatype)
    site_id = site_sensor.split('-')[0]

    # initialize station data response dictionary
    result = {'notes': get_station_notes(f'https://www.sacflood.org/site/?site_id={site_id}', datatype)}

    # check that start/end times are in correct order
    if end_datetime <= start_datetime:
        raise NotImplementedError('Check start and end dates. Start date comes after end date.')

    # print('Retrieving data for {0} from {1:%Y-%m-%d} to {2:%Y-%m-%d}'.format(site_sensor, start_datetime, end_datetime))
    data_df = pd.DataFrame()
    if (end_datetime - start_datetime).days >= 365:
        # print('This may take a while for long period of records')
        for (start, end) in split_date_range_increments_lt_1year(start_datetime, end_datetime):
            site_url = build_sacco_alert_url(start, end, site_sensor, datatype=datatype)
            data_df.append(download_station_data(start,end, site_url, datatype))

    else:
        site_url = build_sacco_alert_url(start_datetime, end_datetime, site_sensor, datatype=datatype)
        data_df = download_station_data(start_datetime, end_datetime, site_url, datatype)

    if as_dataframe:
        result['values'] = data_df
    else:
        result['values'] = data_df.to_dict()

    return result

