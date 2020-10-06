"""
collect.utils.utils
============================================================
The utilities module of MBK Engineers' collect project
"""
# -*- coding: utf-8 -*-
import requests


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
    """
    if datetime_structure.month < 10:
        return datetime_structure.year
    return datetime_structure.year + 1