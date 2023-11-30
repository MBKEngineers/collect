"""
collect.utils.utils
============================================================
The utilities module of MBK Engineers' collect project
"""
# -*- coding: utf-8 -*-
import urllib3
# disable warnings in crontab logs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import urllib3.contrib.pyopenssl
urllib3.contrib.pyopenssl.inject_into_urllib3()

import ssl

import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


# alternate timezone representation depending on Python version
try:
    from zoneinfo import ZoneInfo
    tz_function = ZoneInfo
except:
    from pytz import timezone
    tz_function = timezone


def get_session_response(url, auth=None):
    """
    Arguments:
        url (str): valid web URL
    Returns:
        (requests.models.Response): the response object with site content specified by URL
    """
    session = requests.Session()
    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session.get(url, auth=auth, verify=ssl.CERT_NONE)


def get_web_status(url):
    """
    check status of a URL
    """  
    try:
        response = requests.get(url)
        # Raises a HTTPError if the status is 4xx, 5xx
        response.raise_for_status()  
    
    # connection error or timeout
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        return False
    
    # 4xx or 5xx error
    except requests.exceptions.HTTPError:
        return False
    
    # site is up and working
    else:
        return True


def clean_fixed_width_headers(columns):
    """ 
    for dataframe column headers defined as multi-level index,
    collapsed headers into human-readable names 
    """
    headers = []
    for column in columns:
        column = list(column)
        for i in range(len(column)):
            if 'Unnamed' in column[i]:
                column[i] = ''
        headers.append(' '.join(column).strip())
    return headers


def get_water_year(datetime_structure):
    """
    Returns water year of current datetime object.

    Arguments:
        datetime_structure (datetime.datetime): a Python datetime
    Returns:
        water_year (int): the water year for the provided datetime
    """
    if datetime_structure.month < 10:
        return datetime_structure.year
    return datetime_structure.year + 1


def get_localized_datetime(naive_datetime, timezone_string):
    """
    provides cross-version support for python versions before existence of zoneinfo module

    Arguments:
        naive_datetime (datetime.datetime): a datetime without any timezone information
        timezone_string (str): the string identifier for the desired timezone (i.e. 'UTC' or 'US/Pacific')
    Returns:
        result (datetime.datetime): a python datetime structure with timezone localization
    """
    try:
        expected_tz = timezone(timezone_string)
        result = expected_tz.localize(naive_datetime)
    except:
        result = naive_datetime.replace(tzinfo=ZoneInfo(timezone_string))
    return result
