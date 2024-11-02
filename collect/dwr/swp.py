"""
collect.dwr.swp
======================================================================
access select DWR delta conditions PDFs and State Water Project files
"""
# -*- coding: utf-8 -*-
import datetime as dt
import io
import re

import pandas as pd
from collect import utils


try:
    import pdftotext
except:
    print('Module pdftotext is required for SWP report collection.  Install with `pip install pdftotext==2.2.2`')


def get_report_catalog(console=True):
    """
    prints list of available SWP report names to console
    https://water.ca.gov/Programs/State-Water-Project/Operations-and-Maintenance/Operations-and-Delta-Status
    Arguments:
        console (bool): whether to print catalog to console
    Returns:
        catalog (dict): nested dictionary of report names and associated URLs
    """
    oco_url_base = '/'.join(['https://water.ca.gov/-/media',
                             'DWR-Website',
                             'Web-Pages',
                             'Programs',
                             'State-Water-Project',
                             'Operations-And-Maintenance',
                             'Files',
                             'Operations-Control-Office',
                             ''])
    cnra_url_base = '/'.join(['https://data.cnra.ca.gov',
                              'dataset',
                              '742110dc-0d96-40bc-8e4e-f3594c6c4fe4',
                              'resource',
                              '45c01d10-4da2-4ebb-8927-367b3bb1e601',
                              'download',
                              ''])

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
            'Delta Operations Summary (daily)':
                f'{oco_url_base}Delta-Status-And-Operations/Delta-Operations-Daily-Summary.pdf',
            'Water Quality Summary (daily)':
                f'{oco_url_base}Delta-Status-And-Operations/Delta-Water-Quality-Daily-Summary.pdf',
            'Hydrologic Conditions Summary (daily)':
                f'{oco_url_base}Delta-Status-And-Operations/Delta-Hydrologic-Conditions-Daily-Summary.pdf',
            'Miscellaneous Monitoring Data (daily)':
                f'{oco_url_base}Delta-Status-And-Operations/Delta-Miscellaneous-Daily-Monitoring-Data.pdf',
            'Barker Slough Flows (weekly)': f'{oco_url_base}Delta-Status-And-Operations/Barker-Slough-Weekly-Flows.pdf'
        },
        'Oroville Operations': {
            'Forecasted Storage': f'{oco_url_base}Oroville-Operations/Oroville-Forecasted-Storage.pdf',
            'Hatchery and Robinson Riffle Daily Average Water Temperature':
                f'{oco_url_base}Oroville-Operations/Hatchery-and-Robinson-Riffle-Daily-Average-Water-Temperature.pdf',
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
        report (str): designates which report to retrieve
    Returns:
        url (str): the path to the PDF report
    """
    # flatten the catalog
    flat = {k: v for d in get_report_catalog(console=False).values() for k, v in d.items() }

    # look up the URL by the name of the report
    return flat.get(report)


def get_raw_text(report, filename=None, preserve_white_space=True):
    """
    extract text data from a PDF report on the SWP website

    Arguments:
        filename (str): optional filename (.txt) for raw report export
    Returns:
        content (str): the string contents of the PDF (preserves whitespace)
    Raises:
        ValueError: if the specified report does not map to a PDF, raise a ValueError
    """
    # construct URL
    url = get_report_url(report)

    if not url.endswith('.pdf'):
        raise ValueError(f'ERROR: {report} is not PDF-formatted')

    # request report content from URL
    with io.BytesIO(utils.get_session_response(url).content) as buf:

        # parse PDF and extract as string
        content = pdftotext.PDF(buf, raw=False, physical=True)[0]

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


def get_delta_daily_data(export_as='dict'):
    """
    fetch and return SWP OCO's daily delta operations report

    Arguments:
        export_as (str): designates which format to use for returned data
    Returns:
        result (dict): the report contents and metadata
    """
    content = get_raw_text('Delta Operations Summary (daily)', 'raw_export.txt')

    # extract current report's date
    rx = re.compile(r'(?P<date>\d{1,2}/\d{1,2}/\d{4})')
    date = rx.search(content).group('date')

    # parse the report date
    date_reformat = dt.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')

    # report information
    meta =  {
        'date': date_reformat,
        'filename': 'Delta-Operations-Daily-Summary.pdf',
        'title': 'EXECUTIVE OPERATIONS SUMMARY ON {}'.format(date),
        'contact': 'OCO_Export_Management@water.ca.gov',
        'retrieved': dt.datetime.now().strftime('%Y-%m-%d'),
        'raw': content,
    }

    # pattern to match delta and storage variables
    rx = re.compile(r'(?:\s+)(?P<key>.*)\s+(?P<operator>=|\~{1}|>|<)\s+(?P<value>.+)')

    def _parse_entry(match):
        value = match.group('value')

        units_match = re.findall(r'(cfs|TAF|km|%|% \(14-day avg\))$', value)
        units = units_match[0] if bool(units_match) else ''

        if bool(units):
            value = value.rstrip(units).strip().replace(',', '')

        if match.group('operator') == '=':
            if bool(units):
                return {'value': float(value), 'units': units}
            return {'value': value, 'units': units}
        return {'value': ' '.join([match.group('operator'), value]), 'units': units}

    # extract all groups matching this regular expression pattern
    extract = {match.group('key').strip(): _parse_entry(match)
               for match in rx.finditer(content)}

    # structured dictionary template organizes the categories of the report
    result = {
        # 'date': date_reformat,
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

    # create dataframe
    df = pd.DataFrame(index=[date_reformat])

    # update nested dictionary from extraction
    for x, v in result.items():
        if isinstance(v, dict):
            for k in v.keys():
                v.update({k: extract[k]})

                # update frame
                df[x, k, extract[k]['units']] = extract[k]['value']

    df.columns = pd.MultiIndex.from_tuples(df.columns)

    # return formatted report extraction
    return {'info': meta, 'data': result if export_as == 'dict' else df}


def get_barker_slough_data():
    """
    fetch and return SWP OCO's Barker Slough Flows (weekly) report

    Arguments:
        report (str): designates which report to retrieve
    Returns:
        result (dict): the report contents and metadata
    """
    content = get_raw_text('Barker Slough Flows (weekly)')

    # report information
    meta = {
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


def get_oco_tabular_data(report):
    """
    support for Hydrologic Conditions Summary (daily) and Miscellaneous Monitoring Data (daily)
    reports; combines multi-page reporting into single date-indexed dataframe

    Arguments:
        report (str): designates which report to retrieve
    Returns:
        content (str): the string contents of the PDF (preserves whitespace)
    """
    # construct URL
    url = get_report_url(report)

    # request report content from URL
    with io.BytesIO(utils.get_session_response(url).content) as buf:

        # parse PDF and extract as string
        content = list(pdftotext.PDF(buf, raw=False, physical=True))

    # report information
    meta =  {
        'filename': url.split('/')[-1],
        # 'title': content[0].splitlines()[0],
        'contact': 'OCO_Export_Management@water.ca.gov',
        'retrieved': dt.datetime.now().strftime('%Y-%m-%d'),
        'raw': content,
        'pages': len(content)
    }

    def _process_page(i, page, report):
        """
        Arguments:
            i (int): the index of the page
            page (str): the text content of the page
        Returns:
            df (pandas.DataFrame): tabular results as dataframe
        """
        # strip leading white space, filter out empty rows, and split rows
        # based variable # of whitespace characters (2 or more)
        page = page.replace(',', '')
        rows = [re.split(r'\s{2,}', x.lstrip())
                for x in page.splitlines() if bool(x)]

        # convert table to a dataframe
        df = pd.DataFrame(rows)

        # fill missing column entry (first line of date header)
        if report == 'Miscellaneous Monitoring Data (daily)':
            rows[2] = [''] + rows[2]
            rows[4][0] = '(30 days)'
            df.columns = [' '.join(list(x)).strip() for x in zip(*rows[2:5])]

        elif report == 'Water Quality Summary (daily)':
            if i == 0:
                return pd.DataFrame()
            # delta water quality conditions (page 2)
            if i == 1:
                rows[2][0] = 'Date (30 days)'
                rows[2][2] = 'ANT Half'
                rows[2].insert(3, 'PCT@64km mdEC')
                df.columns = rows[2]
            # delta water quality conditions (page 3)
            elif i == 2:
                rows[2][0] = 'Date (30 days)'
                df.columns = rows[2]
            # delta water quality conditions (page 4)
            elif i == 3:
                rows[2][0] = 'Date (30 days)'
                df.columns = rows[2]
            # south delta stations (page 5)
            elif i == 4:
                rows[3][0] = 'Date (30 days)'
                df.columns = rows[3]
            # Suisun marsh stations (page 6)
            elif i == 5:
                rows[3][0] = 'Date (30 days)'
                df.columns = rows[3]

        # page 1 of hydrology report
        elif i == 0:
            rows[2] = [''] + rows[2]
            rows[4] = [''] + rows[4]
            rows[6] = ['Date (30 days)'] + rows[6]
            df.columns = [' '.join(list(x)).strip() for x in zip(*[rows[2], rows[4], rows[6]])]

        # page 2 of hydrology report
        elif i == 1:
            rows[2] = [''] + rows[2]
            rows[3][0] = 'Date (30 days)'
            df.columns = [' '.join(list(x)).strip() for x in zip(*[rows[2], rows[3]])]

        else:
            raise NotImplementedError('Report is expected to include a maximum of 2 pages.')

        # filter for date format and set date index
        df = df.loc[df['Date (30 days)'].str.match(r'\d{2}/\d{2}/\d{4}')]
        df.set_index('Date (30 days)', drop=True, inplace=True)
        df.index = pd.to_datetime(df.index)

        # filter out all rows/columns where all entries are null/None
        df = df.dropna(axis=0, how='all').dropna(axis=1, how='all')

        return df

    # process multiple pages
    frames = [_process_page(i, page, report) for i, page in enumerate(content)]

    # return string content
    return {'info': meta, 'data': pd.concat(frames, axis=1)}
