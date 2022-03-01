"""
collect.alert.alert
============================================================
Sacramento County Alert system
"""
# -*- coding: utf-8 -*-
import datetime as dt
import urllib2
import os
import re
import pickle
import string

from bs4 import BeautifulSoup, SoupStrainer
from cookielib import CookieJar
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
# from ulmo.util import  open_file_for_url
from urlparse import urljoin


def get_stations(as_dataframe=True, datatype='stream'):
    """
    fetches stream stage stations information from Sacramento County ALERT website
    
    Arguments:
        as_dataframe (bool): This determines what format the stations information is returned as. If ``False`` (default), the stations will be a dict with station ids as keys mapped to a dict of station variables. If ``True`` then the entire sites_dict will be a pandas.DataFrame object containing the equivalent information.
        datatype (str): either 'stream' or 'rain' for stream flow data or rain gage data, respectively
    Returns:
        sites_dict (dict): a dictionary containing station information
    """
    main_alert_page = 'http://www.sacflood.org/home.php'
    if datatype == 'stream':
        measure = 'level'
        group_type_id = 19
    elif datatype == 'rain':
        measure = 'rain'
        group_type_id = 14

    sites_url = f'http://www.sacflood.org/{measure}.php?&view_id=1&group_type_id={group_type_id}&grouping_id='
    # https://www.sacflood.org/list/

    cj = CookieJar()
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
    # main_response = opener.open(main_alert_page)
    sites_response = opener.open(sites_url)
    sites_text = sites_response.read()
    soup_sites = BeautifulSoup(sites_text, 'lxml')

    sites_df = pd.read_html(str(soup_sites.find('table')))[0]

    # strip whitespace from columns
    sites_df.columns = [x.strip() for x in sites_df.columns]

    for x in sites_df.columns:
        first_entry = sites_df[x][0]
        if isinstance(first_entry, str):
            if not all(x in string.printable for x in first_entry):
                sites_df[x] = sites_df[x].apply(lambda x: x[:-1])

    # return
    if as_dataframe:
        return sites_df
    else:
        sites_dict = sites_df.to_dict()
    return sites_dict


def strip_timeouts(stations_df):
    timeout_funk = lambda x: x.split('Timeout')[0].strip()
    stations_df['Site'] = stations_df['Site'].apply(timeout_funk)
    return stations_df


def get_station_data(start_datetime, end_datetime, site_sensor, ascending=False, as_dataframe=True, datatype='stream'):
    """
    retrieves Sacramento County ALERT stream sensor data
    
    Arguments:
        start_datetime (datetime.datetime): Date of start of information desired.  This will start at midnight if time is not otherwise set
        end_datetime (datetime.datetime): Date of end of information desired.  This will end the minute before midnight if time is nototherwise set.
        station_sensor (str): Site & Sensor id's combined with '-' to identify site and sensor from website (e.g. Site = 'Alpine/Unionhouse'; Sensor = 'Unionhouse Creek';  site_sensor='Alpine/Unionhouse-Unionhouse Creek')
        ascending (bool): If True then data ordered from start_date to end_date; if False (default) then data ordered from end date to start date.
        as_dataframe (bool): If True then 'values' key of the returned station dictionary will be a Pandas DataFrame of data    
    Returns:
        station_dictionary (dict): dictionary of station notes and sensor readings
    """
    station_dict = {}
    datatype = {'streamgage': 'stream', 'precip': 'precipitation'}.get(datatype, datatype)

    # check that start/end times are in correct order
    if end_datetime <= start_datetime:
        print('Check start and end dates. Start date comes after end date.')
        return None

    print('Retrieving data for {0} from {1:%Y-%m-%d} to {2:%Y-%m-%d}'.format(site_sensor, start_datetime, end_datetime))
    if (end_datetime - start_datetime).days >= 365:
        print('This may take a while for long period of records')
        new_st_end_dates = split_date_range_increments_lt_1year(start_datetime,end_datetime)
        for date_range in new_st_end_dates:
            st_dt = date_range[0]
            end_dt = date_range[1]
            site_url = build_sacco_alert_url(st_dt,end_dt,site_sensor, datatype=datatype)
            data_exists = check_for_no_data(site_url)
            if data_exists:
                data_df = download_station_data(st_dt,end_dt,site_url, datatype)
                if 'site_notes' not in locals():
                    site_notes = get_station_notes(site_url, datatype)
            else:
                print('Data does not exist for url:\n{0}'.format(site_url))
                break
            if 'entire_data_df' in locals():
                entire_data_df = entire_data_df.append(data_df)
                pass
            else:
                entire_data_df = data_df
    else:
        site_url = build_sacco_alert_url(start_datetime, end_datetime, site_sensor, datatype=datatype)
        data_exists = check_for_no_data(site_url)
        if data_exists:
            data_df = download_station_data(start_datetime, end_datetime, site_url, datatype)
            print('       +got data')
            try:
                site_notes = get_station_notes(site_url, datatype)
                print('       +got notes')
                station_dict['notes'] = site_notes
            except:
                station_dict['notes'] = ''
                print('No notes')
        else:
            print('Data does not exist for url:\n{0}'.format(site_url))
            return None
    if 'entire_data_df' in locals():
        if as_dataframe:
            station_dict['values'] = entire_data_df
        else:
            station_dict['values'] = entire_data_df.to_dict()
    else:
        if as_dataframe:
            station_dict['values'] = data_df
        else:
            station_dict['values'] = data_df.to_dict()
    return station_dict


def download_station_data(start_datetime, end_datetime, data_url, datatype, ascending=False):
    if 'soup_next' in locals():
        del soup_next
    web_text = open_url_get_text(data_url)
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
                    hsplit = h4.text.split('ft')
                    current_reading = hsplit[0].strip()
                    dt_text = hsplit[1].strip()
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
                elm = soup_all.find('a',{'title':'Next'})
                elm_link = elm['href']
                elm_replace = elm['href']
            else:
                elm_replace = elm_link.replace('page=2', f'page={page_number:.0f}')
                elm = elm_link.split('&')[0]
            next_page_link = base_url + elm_replace
            next_page_exists = check_for_no_data(next_page_link)
            if next_page_exists is False:
                next_page = False

            next_text = open_url_get_text(next_page_link)
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
    
    
def create_st_url_datetime(dt, beginning_of_day=True):
    dt_str = dt.strftime('%Y-%m-%d+%H=%M=%S')
    url_dt = dt_str.replace('=', '%3A')
    return url_dt


def create_end_url_datetime(dt, end_of_day=True):
    dtt = dt.datetime(dt.year,dt.month,dt.day,23,59,59)
    dt_str = dt.strftime('%Y-%m-%d+%H=%M=%S')
    url_dt = dt_str.replace('=', '%3A')
    return url_dt


def get_or_create_stn_attr():
    filepath = os.path.abspath(os.path.join(__file__ , '..'))
    stn_attr_pickle_path = os.path.join(filepath, 'sacco_alert_stn_attr.p')
    try:
        mod_date = modification_date(stn_attr_pickle_path)
        if mod_date < dt.datetime.now() - relativedelta(hours=23):
            station_attributes = get_site_url_attr_quick()
            pickle.dump(station_attributes, open(stn_attr_pickle_path, 'wb'))    
        else:
            station_attributes = pickle.load(open(stn_attr_pickle_path, 'rb'))
    except OSError:
        station_attributes = get_site_url_attr_quick()
        pickle.dump(station_attributes, open(stn_attr_pickle_path, 'wb'))
    return station_attributes


def build_sacco_alert_url(start_datetime, end_datetime, site_sensor, datatype='stream'):
    """
    Sacramento County ALERT stream sensors urls to download data

    Arguments:
        start_datetime (datetime.datetime): starting datetime for download of data
        end_datetime (datetime.datetime): ending datetime for download of data
        station_sensor (str): Site & Sensor id's combined with '-' to identify site and sensor from website (e.g. Site = 'Alpine/Unionhouse'; Sensor = 'Unionhouse Creek'; site_sensor='Alpine/Unionhouse-Unionhouse Creek')
    Returns:
        data_url (str): query url
    """
    site_sensor = site_sensor.split('-')[0] if datatype == 'precipitation' else site_sensor
    site_attr = get_site_url_attr_quick()[datatype][site_sensor]
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
                    'data_start={0}'.format(create_st_url_datetime(start_datetime)),
                    'data_end={0}'.format(create_end_url_datetime(end_datetime))]
    data_url = 'http://www.sacflood.org/sensor.php?' + '&'.join(query_params)
    return data_url


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


def modification_date(filename):
    t = os.path.getmtime(filename)
    return dt.datetime.fromtimestamp(t)


def get_or_create_lat_lon():
    weekago = dt.datetime.now() - relativedelta(months=11,days=6,hours=23)
    taskapp_path =  os.path.abspath(os.path.join(__file__, '..'))
    lat_lon_pickle_path = os.path.join(taskapp_path, 'sacco_alert_lat_lon.p')
    try:
        mod_date = modification_date(lat_lon_pickle_path)
        if mod_date < weekago:
            sacco_lat_lon = get_lat_lon()
            pickle.dump(sacco_lat_lon, open(lat_lon_pickle_path, 'wb'))
        else:
            sacco_lat_lon = pickle.load(open(lat_lon_pickle_path, 'rb'))
    except OSError:
        sacco_lat_lon = get_lat_lon()
        pickle.dump(sacco_lat_lon, open(lat_lon_pickle_path, 'wb'))
    return sacco_lat_lon


def get_lat_lon():
    main_page_string_all = 'https://www.sacflood.org/home.php'
    main_page_text = open_url_get_text(main_page_string_all)
    soup_all = BeautifulSoup(main_page_text, 'lxml')
    soup_links = soup_all.find_all('a', href=True)
    counter = 0
    system_wide_link_endings = []
    lat_lon_dict = {}
    for link in soup_links:
        if link.next_element == 'System Wide':
            counter += 1
            if counter <= 2:
                system_wide_link_endings.append(link['href'])
    for datatype in ['stream', 'precipitation']:
        stations_df = get_stations(as_dataframe=True, datatype=datatype)
        # stn_items = stations_df.iteritems()
        # station_attributes = {}
        for link_end in system_wide_link_endings:
            actual_link_end = link_end.split('..')[-1]
            system_wide_url = urljoin(main_page_string_all,actual_link_end)
            system_wide_text = open_url_get_text(system_wide_url)
            soup_system_wide = BeautifulSoup(system_wide_text, 'lxml')
            sys_wide_soup_links = soup_system_wide.find_all('a', href=True)
            station_rows = stations_df.iterrows()
            for stn_row in station_rows:
                station = '{0}'.format(stn_row[1]['Site'])
                sensor = '{0}'.format(stn_row[1]['Sensor'])
                stn_sen = '{0}-{1}'.format(station,sensor)
                # station_attributes[station] = {}
                if 'Timeout' in station:
                    station = station.split('Timeout')[0].strip()
                for stn_link in sys_wide_soup_links:
                    if stn_link.next_element == '{0}'.format(station):
                        station_url_ending = stn_link['href']
                        station_url = urljoin(main_page_string_all,station_url_ending)
                        station_text = open_url_get_text(station_url)
                        pattern = '<strong>Latitude: </strong>(.*)<strong>Longitude: </strong>(.*)</a></span>'
                        if '(' in station:
                            station_pattern = station.replace('(', '\(').replace(')', '\)')
                        else:
                            station_pattern = station
                        pattern2 = '<title>Sac County ALERT System Site:{0}\((.*)\)</title>'.format(station_pattern)
                        try:
                            lat_lon_dict[station] = {}
                            lat_lon = re.findall(pattern,station_text)
                            lat = lat_lon[0][0].strip()
                            lon = lat_lon[0][1].strip()
                            lat_lon_dict[station]['lat'] = lat
                            lat_lon_dict[station]['lon'] = lon
                            stn_id_number = re.findall(pattern2,station_text)
                            station_id_number = stn_id_number[0]
                            lat_lon_dict[station]['id_number'] = station_id_number
                        except:
                            print('Station {0} does NOT contain LAT or LON'.format(station))
    return lat_lon_dict


def get_site_url_attr_quick():
    """
    datatype ['stream', 'precipitation']
    """
    datatypes = ['stream', 'precipitation']
    station_attributes = {}
    for datatype in datatypes:
        taskapp_path =  os.path.abspath(os.path.join(__file__, '..'))
        stn_attr_pickle_path = os.path.join(taskapp_path, 'sacco_alert_stn_attr.p')
        html_junk = '&lt;/strong&gt; &lt;small&gt;'
        html_junk4 = '&lt;strong&gt'
        html_junk2 = '&lt;br/&gt;'
        html_junk3 = '&lt;/small&gt'
        if datatype == 'stream':
            map_url = 'https://www.sacflood.org/map.php?&map_id=2&view_id=1&sensor_class=20&mode=sensor'
            pattern = '\<a href="(.*)"\>\<img src=".*" width="13" height="13"  alt=""data-toggle="tooltip" title="\<p\>{3};(.*){0}\((.*)\){2};{1}{3};(.*){0}\('.format(html_junk,html_junk2,html_junk3,html_junk4)
        elif datatype == 'precipitation':
            map_url = 'https://www.sacflood.org/map.php?&map_id=2&view_id=1&sensor_class=10&mode=accumulation&interval=1440'
            pattern = '\<a href="(.*)"\>\<img.*src=".*" width="13" height="13"  alt=""data-toggle="tooltip" title="\<p\>{3};(.*){0}\((.*)\){2};{1}.*{1}(.*)\</p\>" /\>\<span class'.format(html_junk,html_junk2,html_junk3,html_junk4)
        else:
            print('Please use either "stream" or "precipitation" for datatype')
        map_page_text = open_url_get_text(map_url)
        stn_attr_pickle = open_file_for_url(map_url, stn_attr_pickle_path, check_modified=True)

        site_attributes = re.findall(pattern, map_page_text)
        station_attributes[datatype] = {}
        for site_attr in site_attributes:
            site_url_end = site_attr[0]
            site_name = site_attr[1]
            site_numb = site_attr[2]
            sensor_name = site_attr[3]
            site_sensor_name = '{0}-{1}'.format(site_name, sensor_name)
            if datatype == 'stream':
                station_attributes[datatype][site_sensor_name] = {}
            else:
                station_attributes[datatype][site_name] = {}
            attr_list = site_url_end.split('?')[-1].split('&amp;')
            for attr in attr_list:
                attr_name = attr.split('=')[0]
                attr_value = attr.split('=')[-1]
                if datatype == 'stream':
                    station_attributes[datatype][site_sensor_name][attr_name] = attr_value
                else:
                    station_attributes[datatype][site_name][attr_name] = attr_value
    return station_attributes


def get_website_url_links_from_text_label(url, website_text_label=None):
    """
    Note: website_text_label is a list.  This allows for multiple elements to be returned.
    Also note this will return all urls matching the website_text_label for a given url
    """
    url_soup = BeautifulSoup(url, 'lxml')
    url_links = url_soup.find_all('a', href=True)
    desired_links = []
    if website_text_label is not None:
        for url_link in url_links:
            if url_link.next_element in website_text_label:
                desired_links.append(url_link['href'])
    else:
        for url_link in url_links:
            desired_links.append(url_link['href'])
    return desired_links


def check_for_no_data(data_url): 
    soup = BeautifulSoup(open_url_get_text(data_url), 
                         'lxml', 
                         parse_only=SoupStrainer('div', {'class': 'app-alert fade in alert alert-info'}))
    if 'No data' in soup.text:
        return False
    else:
        return True


def open_url_get_text(data_url): 
    main_alert_page = 'http://www.sacflood.org/home.php'
    cj = CookieJar()
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
    # main_response = opener.open(main_alert_page)
    data_response = opener.open(data_url)
    return data_response.read()


def get_station_notes(data_url, datatype):
    notes_dict = {}
    web_text = open_url_get_text(data_url)
    
    if datatype == 'stream':
        soup = BeautifulSoup(web_text, 
                             'lxml', 
                             parse_only=SoupStrainer('p', {'class': 'list-group-item-text'}))
        for nh in soup.findAll('strong'):
            header = nh.text
            header_note_raw = nh.nextSibling
            header_note = header_note_raw.split('&nbsp;')[-1]
            notes_dict[header] = header_note
    else:
        soup_all = BeautifulSoup(web_text, 'lxml')
        notes_df = pd.read_html(str(soup_all.find('table')),header=1,index_col=0)[0]
        notes_dict['precip_freq_est_inches'] = notes_df
    return notes_dict





if __name__ == '__main__':
    start_datetime = dt.datetime(2015,12,25)
    end_datetime = dt.datetime(2016,1,31)
    stn_attr = get_all_url_attr_thru_linx()
    # station_df = get_stations(datatype='rain',as_dataframe=True)
    # import pdb;pdb.set_trace()
    # data_df = get_station_data(start_datetime,end_datetime,'Alpine/Unionhouse-Unionhouse Creek')