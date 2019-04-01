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