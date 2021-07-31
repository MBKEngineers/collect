"""
collect.dwr.delta_conditions
============================================================
access DWR Delta Conditions PDF
"""
# -*- coding: utf-8 -*-
import datetime as dt
import io
import re

import pandas as pd
import pdftotext
import requests


def get_report_catalog(console=True):
    """
    prints list of available SWP report names to console
    https://water.ca.gov/Programs/State-Water-Project/Operations-and-Maintenance/Operations-and-Delta-Status
    Arguments:
        console (bool): whether to print catalog to console
    Returns:
        catalog (dict): nested dictionary of report names and associated URLs
    """
    oco_url_base = 'https://water.ca.gov/-/media/DWR-Website/Web-Pages/Programs/State-Water-Project/Operations-And-Maintenance/Files/Operations-Control-Office/'
    cnra_url_base = 'https://data.cnra.ca.gov/dataset/742110dc-0d96-40bc-8e4e-f3594c6c4fe4/resource/45c01d10-4da2-4ebb-8927-367b3bb1e601/download/'

    catalog = {
        'Dispatcher\'s Daily Water Reports': {
            'Mon': f'{cnra_url_base}dispatchers-monday-water-report.txt',
            'Tues': f'{cnra_url_base}dispatchers-tuesday-water-report.txt',
            'Wed': f'{cnra_url_base}dispatchers-wednesday-water-report.txt',
            'Thu': f'{cnra_url_base}dispatchers-thursday-water-report.txt',
            'Fri': f'{cnra_url_base}dispatchers-friday-water-report.txt',
            'Sat': f'{cnra_url_base}dispatchers-saturday-water-report.txt',
            'Sun': f'{cnra_url_base}dispatchers-sunday-water-report.txt',
        },
        'Delta Status and Operations': {
            'Delta Operations Summary (daily)': f'{oco_url_base}Delta-Status-And-Operations/Delta-Operations-Daily-Summary.pdf',
            'Water Quality Summary (daily)': f'{oco_url_base}Delta-Status-And-Operations/Delta-Water-Quality-Daily-Summary.pdf',
            'Hydrologic Conditions Summary (daily)': f'{oco_url_base}Delta-Status-And-Operations/Delta-Hydrologic-Conditions-Daily-Summary.pdf',
            'Miscellaneous Monitoring Data (daily)': f'{oco_url_base}Delta-Status-And-Operations/Delta-Miscellaneous-Daily-Monitoring-Data.pdf',
            'Barker Slough Flows (weekly)': f'{oco_url_base}Delta-Status-And-Operations/Barker-Slough-Weekly-Flows.pdf',
            'Forecasted Storage': f'{oco_url_base}Oroville-Operations/Oroville-Forecasted-Storage.pdf',
            'Hatchery and Robinson Riffle Daily Average Water Temperature': f'{oco_url_base}Oroville-Operations/Hatchery-and-Robinson-Riffle-Daily-Average-Water-Temperature.pdf',
        },
        'Weekly Reservoir Storage Charts': {
            'Oroville': f'{oco_url_base}Project-Wide-Operations/Oroville-Weekly-Reservoir-Storage-Chart.pdf',
            'Del Valle': f'{oco_url_base}Project-Wide-Operations/Del-Valle-Weekly-Reservoir-Storage-Chart.pdf',
            'San Luis': f'{oco_url_base}Project-Wide-Operations/San-Luis-Weekly-Reservoir-Storage-Chart.pdf',
            'Pyramid': f'{oco_url_base}Project-Wide-Operations/Pyramid-Weekly-Reservoir-Storage-Chart.pdf',
            'Castaic': f'{oco_url_base}Project-Wide-Operations/Castaic-Weekly-Reservoir-Storage-Chart.pdf',
            'Silverwood': f'{oco_url_base}Project-Wide-Operations/Silverwood-Weekly-Reservoir-Storage-Chart.pdf',
            'Perris': f'{oco_url_base}Project-Wide-Operations/Perris-Weekly-Reservoir-Storage-Chart.pdf',
        },
        'Weekly Summaries': {
            'Weekly Summary of SWP Water Operations': f'{cnra_url_base}summary-of-water-operations.txt',
            'Weekly Summary of SWP Reservoirs': f'{cnra_url_base}summary-of-reservoirs.txt',
        }
    }
    if console:
        for k, v in catalog.items():
            print('\n')
            print(k)
            print('='*80)
            for kk, vv in v.items():
                print('\t'+kk)
                print('\t\t'+vv.split('/')[-1])

    return catalog


def get_report_url(report):
    """
    
    Arguments:

    Returns:
        url (str): the path to the PDF report
    """
    url_base = 'https://water.ca.gov/-/media/DWR-Website/Web-Pages/Programs/State-Water-Project/'
    swp_base = url_base + 'Operations-And-Maintenance/Files/Operations-Control-Office/Delta-Status-And-Operations'
    url = '/'.join([swp_base, 'Delta-Operations-Daily-Summary.pdf'])

    # get_report_catalog(console=False)['Delta Status and Operations']['Hydrologic Conditions Summary (daily)']

    return get_report_catalog(console=False)['Delta Status and Operations']['Barker Slough Flows (weekly)']


def get_raw_text(report, filename=None, preserve_white_space=True):
    """
    Arguments:
        filename (str): optional filename (.txt) for raw report export
    Returns:
        content (str): the string contents of the PDF (preserves whitespace)
    """
    # construct URL
    url = get_report_url(report)

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


def get_delta_daily_data(report):
    """
    fetch and return SWP OCO's daily delta operations report
    
    Arguments:
        report (str): designates which report to retrieve
    Returns:
        result (dict): the report contents and metadata
    """
    content = get_raw_text(report, 'raw_export.txt')

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


def get_barker_slough_data(report):
    """
    fetch and return SWP OCO's weekly Barker Slough flows report
    
    Arguments:
        report (str): designates which report to retrieve
    Returns:
        result (dict): the report contents and metadata
    """
    content = get_raw_text(report)

    # report information
    meta =  {
        'filename': 'Barker-Slough-Weekly-Flows.pdf',
        'title': content.splitlines()[0],
        'contact': 'OCO_Export_Management@water.ca.gov',
        'retrieved': dt.datetime.now().strftime('%Y-%m-%d'),
        'raw': content,
    }

    # strip leading white space, filter out empty rows, and split rows 
    # based variable # of whitespace characters (2 or more)
    rows = [re.split(r'\s{2,}', x.lstrip())
            for x in content.splitlines() if bool(x)]

    # convert table to a date-indexed dataframe
    df = pd.DataFrame(rows[3:], columns=rows[2])
    df.set_index('Date', drop=True, inplace=True)
    df.index = pd.to_datetime(df.index)
    
    # return result
    return {'info': meta, 'data': df}
