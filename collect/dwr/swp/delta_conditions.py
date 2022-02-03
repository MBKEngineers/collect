"""
collect.dwr.delta_conditions
============================================================
access DWR Delta Conditions PDF
"""
# -*- coding: utf-8 -*-
import datetime as dt
import io
import re

import pdftotext
import requests


def get_report_catalog():
    """
    prints list of available SWP report names to console

    Returns:
        None
    """
    return 'Delta Operations Summary (daily)'


def get_report_url():
    """
    
    Arguments:

    Returns:
        url (str): the path to the PDF report
    """
    url_base = 'https://water.ca.gov/-/media/DWR-Website/Web-Pages/Programs/State-Water-Project/'
    swp_base = url_base + 'Operations-And-Maintenance/Files/Operations-Control-Office/Delta-Status-And-Operations'
    url = '/'.join([swp_base, 'Delta-Operations-Daily-Summary.pdf'])

    return url


def get_raw_text(filename=None, preserve_white_space=True):
    """
    Arguments:
        filename (str): optional filename (.txt) for raw report export
    Returns:
        content (str): the string contents of the PDF (preserves whitespace)
    """
    # construct URL
    url = get_report_url()

    # request report content from URL
    f = io.BytesIO(requests.get(url).content)
    f.seek(0)

    # parse PDF and extract as string
    content = pdftotext.PDF(f)[0]

    # optionally export the raw report as text
    if filename:
        with open(filename, 'w') as f:

            # optionally strip out indentation and excess white space from text
            if not preserve_white_space:
                content = '\n'.join([str(x).strip() for x in content.splitlines() if bool(x.strip().lstrip('~'))])
            
            # write to file
            f.write(content)

    # return string content
    return content


def get_data():
    """
    fetch and return SWP OCO's daily delta operations report
    
    Arguments:
        
    Returns:
        result (dict): the report contents
    """
    content = get_raw_text('raw_export.txt')

    # extract current report's date
    rx = re.compile(r'(?P<date>\d{1,2}/\d{1,2}/\d{4})')   
    date = rx.search(content).group('date')

    # report information
    meta =  {
        'filename': 'Delta-Operations-Daily-Summary.pdf',
        'title': 'EXECUTIVE OPERATIONS SUMMARY ON {}'.format(date),
        'contact': 'OCO_Export_Management@water.ca.gov',
        'retrieved': dt.datetime.now().strftime('%Y-%m-%d'),
        'raw': content,
    }
    
    # pattern to match delta and storage variables
    rx = re.compile(r'(?:\s+)(?P<key>.*)\s+(?P<operator>=|\~{1}|>|<)\s+(?P<value>.+)')

    # extract all groups matching this regular expression pattern
    extract = {match.group('key').strip(): [match.group('operator'), match.group('value')]
               for match in rx.finditer(content)}

    # structured dictionary template organizes the categories of the report
    result = {
        'Scheduled Exports for Today': {
            'Clifton Court Inflow': [], 
            'Jones Pumping Plant': []
        },
        'Estimated Delta Hydrology': {
            'Total Delta Inflow': [],
            'Sacramento River': [],
            'San Joaquin River': [],
        },
        'Delta Operations': {
            'Delta Conditions': [],
            'Delta x-channel Gates (% of day is open)': [],
            'Outflow Index': [],
            '% Inflow Diverted': [],
            'X2 Position (yesterday)': [],
            'Controlling Factor(s)': [],
            'OMR Index Daily Value': [],
        },
        'Reservoir Storages (as of midnight)': {
            'Shasta Reservoir': [],
            'Folsom Reservoir': [],
            'Oroville Reservoir': [],
            'San Luis Res. Total': [],
            'SWP Share': [],
        },
        'Reservoir Releases': {
            'Keswick': [],
            'Nimbus': [],
            'Oroville': [],
        }
    }

    # update nested dictionary from extraction
    for v in result.values():
        if isinstance(v, dict):
            for k in v.keys():
                v.update({k: extract[k]})

    # parse the report date
    date_reformat = dt.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')

    # add formatted date to dicts
    result.update({'date': date_reformat})
    meta.update({'date': date_reformat})

    # return formatted report extraction
    return {'info': meta, 'data': result}


if __name__ == '__main__':

    print(color(get_data()['info'].keys(), 'magenta'))
