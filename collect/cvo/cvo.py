"""
collect.cvo.common
==========================================================================
functions to support retrieving tabular data from CVO monthly report PDFs
"""
# -*- coding: utf-8 -*-
import calendar
import datetime as dt
import os
import re

import dateutil.parser
import pandas as pd
import requests
try:
    from tabula import read_pdf
except:
    print('WARNING: tabula is required for cvo module')


def get_title(report_type):
    """
    get the title for the identified CVO report

    Arguments:
        report_type (str): one of 'kesdop', 'shadop', 'shafln', 'doutdly', 'fedslu', 'slunit'
    Returns:
        title (str): the report title text
    """
    if report_type == 'shafln':
        return 'Shasta Reservoir Daily Operations'
    elif report_type == 'kesdop':
        return 'Kesdop Reservoir Daily Operations'
    elif report_type == 'shadop':
        return 'Shadop Reservoir Daily Operations'
    elif report_type == 'doutdly':
        return 'U.S. Bureau of Reclamation - Central Valley Operations Office Delta Outflow Computation'
    elif report_type == 'slunit':
        return 'Federal-State Operations, San Luis Unit'
    elif report_type == 'fedslu':
        return 'San Luis Reservoir Federal Daily Operations'
    return ''


def get_url(date_structure, report_type):
    """
    Arguments:
        date_structure (datetime.datetime): datetime representing report year and month
        report_type (str): one of 'kesdop', 'shadop', 'shafln', 'doutdly', 'fedslu', 'slunit'
    Returns:
        url (str): the PDF resource URL for the specified month and report type
    """
    # special handling for delta outflow calculation reports
    if report_type in ['doutdly', 'dout']:

        # current month URL
        if date_structure.strftime('%Y-%m') == dt.date.today().strftime('%Y-%m'):
            return f'https://www.usbr.gov/mp/cvo/vungvari/doutdly.pdf'

        # if date is less than or equal to April 2002, use txt format
        if date_structure <= dt.datetime(2002, 4, 1):
            return f'https://www.usbr.gov/mp/cvo/vungvari/dout{date_structure:%m%y}.txt'

        # if date is less than or equal to December 2010, use prn format
        if date_structure <= dt.datetime(2010, 12, 1):
            return f'https://www.usbr.gov/mp/cvo/vungvari/dout{date_structure:%m%y}.prn'

        # reference pdf format
        return f'https://www.usbr.gov/mp/cvo/vungvari/dout{date_structure:%m%y}.pdf'

    # current month URL
    if date_structure.strftime('%Y-%m') == dt.date.today().strftime('%Y-%m'):
        return f'https://www.usbr.gov/mp/cvo/vungvari/{report_type}.pdf'

    # default report URL for past months
    return f'https://www.usbr.gov/mp/cvo/vungvari/{report_type}{date_structure:%m%y}.pdf'


def months_between(start_date, end_date):
    """
    given two instances of ``datetime.datetime``, generate a list of dates on
    the 1st of every month between the two dates (inclusive).

    Arguments:
        start_date (datetime.datetime):  start date given by user input
        end_date (datetime.datetime): end date given by user input
    Yields:
       (datetime.date): all dates between the date range in mmyy format
    """
    if start_date > end_date:
        raise ValueError(f'Start date {start_date} is not before end date {end_date}')

    year = start_date.year
    month = start_date.month

    while (year, month) <= (end_date.year, end_date.month):
        yield dt.date(year, month, 1)

        # increment date by one month
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1


def data_cleaner(content, report_type, date_structure):
    """
    Changes dataframe to an array and reshape it
    column names change to what is specified below

    This function converts data from string to floats and
    removes any non-numeric elements 

    Arguments:
        content (dataframe):  dataframe in a [1,rows,columns] structure
        report_type (string): name of report
        date_structure (datetime.datetime): 
    Returns:
        df (dataframe): in a [rows,columns] structure, 
    """
    if isinstance(content, list):
        content = content[0]

    # Change from array to dataframe, generate new columns
    df = pd.DataFrame(content if content.ndim <= 2 else content[0])

    # set the multi-level column names
    df.columns = pd.MultiIndex.from_tuples(get_report_columns(report_type, date_structure, expected_length=len(df.columns)))

    # change the dates in dataframe to date objects (if represented as integer days, construct 
    # date with current month/year)
    df = df.set_index(('Date', 'Date'))
    df.index.name = None
    df.index = df.index.map(lambda x: dt.date(date_structure.year, date_structure.month, x) if str(x).isnumeric() 
                                      else dateutil.parser.parse(x) if x != '-' 
                                      else None)

    # convert numeric data to floats
    for key, value in df.iteritems():
        df.loc[:, key] = (value.astype(str)
                               .replace(to_replace=r'[,\/]', value='', regex=True)
                               .replace(to_replace=r'[%\/]', value='', regex=True)
                               # .replace(to_replace=r'[(\/]', value='-', regex=True) # check if we need to convert to negative???
                               # .replace(to_replace=r'[)\/]', value='', regex=True)
                               .replace(to_replace='None', value=float('nan'), regex=True)
                               .astype(float))

    # drop COA columns with no data
    if 'COA USBR' in df:
        if df['COA USBR']['Account Balance'].dropna().empty:
            df.drop('COA USBR', axis=1, inplace=True)

    # return converted dataframe; drop NaN values
    return df.dropna(how='all').reindex()


def get_report_columns(report_type, date_structure, expected_length=None):
    """
    Arguments:
        report_type (str):
        date_structure (datetime.datetime):
        expected_length (int): expected length of columns (axis=1)
    Returns:
        (tuple): tuple of tuples representing multi-level column names
    """
    if report_type == 'doutdly':
        tuples = (('Date', 'Date'),
                  ('DELTA INFLOW', 'Sacto R @Freeport prev dy'),
                  ('DELTA INFLOW', 'SRTP prev dy'),
                  ('DELTA INFLOW', 'Yolo + Misc prev dy'),
                  ('DELTA INFLOW', 'East Side Streams prev dy'),
                  ('DELTA INFLOW', 'S. Joaquin Riv Total prev dy'),
                  ('DELTA INFLOW', 'S. Joaquin Riv Total 7-day Avg'),
                  ('DELTA INFLOW', 'S. Joaquin Riv Total Monthly Avg'),
                  ('DELTA INFLOW', 'Total Delta Inflow'),
                  ('NDCU', 'NDCU'),
                  ('DELTA EXPORTS', 'Clifton Court (CLT)'),
                  ('DELTA EXPORTS', 'Tracy (TRA)'),
                  ('DELTA EXPORTS', 'Contra Costa (CCC)'),
                  ('DELTA EXPORTS', 'Byron Bethany (BBID)'),
                  ('DELTA EXPORTS', 'NBA'),
                  ('DELTA EXPORTS', 'Total Delta Exports'),
                  ('DELTA EXPORTS', '3-day Avg TRA & CLT'),
                  ('OUTFLOW INDEX', 'NDOI daily'),
                  ('OUTFLOW INDEX', '7-day Avg'),
                  ('OUTFLOW INDEX', 'Monthly Avg'),
                  ('EXPORT/INFLOW', 'Daily (%)'),
                  ('EXPORT/INFLOW', '3 Day (%)'),
                  ('EXPORT/INFLOW', '14 Day (%)'),
                  ('COA USBR', 'Account Balance'))
    if report_type == 'dout' or report_type == 'doutdly':
        return (
            ('Date', 'Date'),
            ('Delta Inflow', 'Sacto R @Freeport prev dy'),
            ('Delta Inflow', 'SRTP prev wk'),
            ('Delta Inflow', 'Yolo + Misc prev dy'),
            ('Delta Inflow', 'East Side Streams prev dy'),
            ('Delta Inflow', 'S. Joaquin River @ Vernalis prev dy'),
            ('Delta Inflow', 'S. Joaquin River @ Vernalis 7-day Avg'),
            ('Delta Inflow', 'S. Joaquin River @ Vernalis Monthly Avg'),
            ('Delta Inflow', 'Total Delta Inflow'),
            ('NDCU', 'NDCU'),
            ('Delta Exports', 'Clifton Court (CLT)'),
            ('Delta Exports', 'Tracy (TRA)'),
            ('Delta Exports', 'Contra Costa (CCC)'),
            ('Delta Exports', 'Byron Bethany (BBID)'),
            ('Delta Exports', 'NBA'),
            ('Delta Exports', 'Total Delta Exports'),
            ('Delta Exports', '3-day Avg TRA & CLT'),
            ('Outflow Index', 'NDOI daily'),
            ('Outflow Index', '7-day Avg'),
            ('Outflow Index', 'Monthly Avg'),
            ('Export/Inflow', 'Daily (%)'),
            ('Export/Inflow', '3 Day (%)'),
            ('Export/Inflow', '14 Day (%)'),
            ('COA USBR', 'Account Balance')
        )
    elif report_type == 'shafln':
        return (
            ('Day', ''),
            ('Storage - A.F.', 'Lake Britton'),
            ('Storage - A.F.', 'McCloud Div'),
            ('Storage - A.F.', 'Iron Canyon'),
            ('Storage - A.F.', 'Pit 6'),
            ('Storage - A.F.', 'Pit 7'),
            ('Reservoir Total', 'Reservoir Total'),
            ('Change', 'A.F.'),
            ('Change', 'C.F.S.'),
            ('Shasta Inflow C.F.S.', 'Shasta Inflow C.F.S.'),
            ('Natural River C.F.S.', 'Natural River C.F.S.'),
            ('Accum * Full Natural 1000 A.F.', 'Accum * Full Natural 1000 A.F.')
        )
    elif report_type == 'fedslu':
        return (
            ('Day'),
            ('Elev Feet'),
            ('Storage'), 
            ('Change'), 
            ('Federal Pump'), 
            ('Federal Gen'), 
            ('Pacheco Pump'), 
            ('ADJ'), 
            ('Federal Change'), 
            ('Federal Storage')
        )
    elif report_type == 'slunit':
        tuples = [
            ('Day', '', ''),
            ('AQUEDUCT CHECK 12', 'AQUEDUCT CHECK 12', 'STATE'),
            ('AQUEDUCT CHECK 12', 'AQUEDUCT CHECK 12', 'FED'),
            ('AQUEDUCT CHECK 12', 'AQUEDUCT CHECK 12', 'TOTAL'),
            ('ONEILL', 'PUMPING', 'FED'),
            ('ONEILL', 'PUMPING', 'STATE'),
            ('ONEILL', 'PUMPING', 'TOTAL'),
            ('ONEILL', 'GENER', 'TOTAL'),
            ('SAN LUIS', 'PUMPING', 'FED'),
            ('SAN LUIS', 'PUMPING', 'STATE'),
            ('SAN LUIS', 'PUMPING', 'TOTAL'),
            ('SAN LUIS', 'GENERATION', 'FED'),
            ('SAN LUIS', 'GENERATION', 'STATE'),
            ('SAN LUIS', 'GENERATION', 'TOTAL'),
            ('PACHECO', 'PACHECO', 'PUMP'),
            ('DOS AMIGOS', 'DOS AMIGOS', 'FED'),
            ('DOS AMIGOS', 'DOS AMIGOS', 'STATE'),
            ('DOS AMIGOS', 'DOS AMIGOS', 'TOTAL')
        ]
        # Jun2012 - Dec2013
        if dt.date(2012, 6, 1) <= date <= dt.date(2013, 12, 31):
            tuples += [
                ('AQEDUCT CHECK 21', 'AQEDUCT CHECK 21', 'FED'),
                ('AQEDUCT CHECK 21', 'AQEDUCT CHECK 21', 'STATE'),
                ('AQEDUCT CHECK 21', 'AQEDUCT CHECK 21', 'TOTAL')
            ]
        elif date > dt.date(2013, 12, 31):
            tuples = (
                ('Day', '', '', ''),
                ('AQUEDUCT CHECK 12', 'AQUEDUCT CHECK 12', 'STATE', '1'),
                ('AQUEDUCT CHECK 12', 'AQUEDUCT CHECK 12', 'FED', '2'),
                ('AQUEDUCT CHECK 12', 'AQUEDUCT CHECK 12', 'TOTAL', '3'),
                ('ONEILL', 'PUMPING', 'FED', '4'),
                ('ONEILL', 'PUMPING', 'STATE', '5'),
                ('ONEILL', 'PUMPING', 'TOTAL', '6'),
                ('ONEILL', 'GENER', 'TOTAL', '7'),
                ('SAN LUIS', 'PUMPING', 'FED', '8'),
                ('SAN LUIS', 'PUMPING', 'STATE', '9'),
                ('SAN LUIS', 'PUMPING', 'TOTAL', '10'),
                ('SAN LUIS', 'GENERATION', 'FED', '11'),
                ('SAN LUIS', 'GENERATION', 'STATE', '12'),
                ('SAN LUIS', 'GENERATION', 'TOTAL', '13'),
                ('PACHECO', 'PACHECO', 'PUMP', '14'),

                ('DOS AMIGOS', 'DOS AMIGOS', 'XVC', '15'),
                ('DOS AMIGOS', 'DOS AMIGOS', 'FED', '16'),
                ('DOS AMIGOS', 'DOS AMIGOS', 'STATE', '17'),
                ('DOS AMIGOS', 'DOS AMIGOS', 'TOTAL', '18'),

                ('AQEDUCT CHECK 21', 'AQEDUCT CHECK 21', 'FED', '19'),
                ('AQEDUCT CHECK 21', 'AQEDUCT CHECK 21', 'STATE', '20'),
                ('AQEDUCT CHECK 21', 'AQEDUCT CHECK 21', 'TOTAL', '21'),   
            )
        return tuples

    elif report_type == 'kesdop':
        return (
            ('Day', ''),
            ('ELEV', 'ELEV'),
            ('STORAGE ACRE-FEET', 'RES.'),
            ('STORAGE ACRE-FEET', 'CHANGE'),
            ('COMPUTED* INFLOW C.F.S.', 'COMPUTED* INFLOW C.F.S.'),
            ('SPRING CR. P. P. RELEASE', 'SPRING CR. P. P. RELEASE'),
            ('SHASTA RELEASE C.F.S.', 'SHASTA RELEASE C.F.S.'),
            ('RELEASE - C.F.S.', 'POWER'),
            ('RELEASE - C.F.S.', 'SPILL'),
            ('RELEASE - C.F.S.', 'FISHTRAP'),
            ('EVAP C.F.S. (1)', 'EVAP C.F.S. (1)')
        )
    elif report_type == 'shadop':
        return (
            ('Day', '', ''),
            ('ELEV', 'ELEV', 'ELEV'),
            ('STORAGE', '1000 ACRE-FEET', 'IN LAKE'),
            ('STORAGE', '1000 ACRE-FEET', 'CHANGE'),
            ('COMPUTED* INFLOW C.F.S.', 'COMPUTED* INFLOW C.F.S.', 'COMPUTED* INFLOW C.F.S.'),
            ('RELEASE - C.F.S.', 'RIVER', 'POWER'),
            ('RELEASE - C.F.S.', 'RIVER', 'SPILL'),
            ('RELEASE - C.F.S.', 'RIVER', 'OUTLET'),
            ('EVAPORATION', 'EVAPORATION', 'C.F.S.'),
            ('EVAPORATION', 'EVAPORATION', 'INCHES'),
            ('PRECIP', 'PRECIP', 'INCHES')
        )
    else:
        raise NotImplementedError(f'report_type {report_type} is not supported.')

    if isinstance(expected_length, int):
        return tuples[:expected_length]
    return tuples


def get_pdf_area(date_structure, report_type):
    """
    Arguments:
        date_structure (datetime.datetime): 
        report_type (str): one of 'kesdop', 'shadop', 'shafln'
    Returns:
        area (list): list of boundaries for tabula.read_pdf target area 
    """
    today_date = dt.datetime.now()
    if report_type == 'shafln':
        # set the default bottom boundary for tabula.read_pdf function
        area = [140, 30, 445, 540]

        # Set up a condition that replaces url with correct one each loop
        if date_structure.strftime('%Y-%m-%d') == today_date.strftime('%Y-%m-01'):

            # set the bottom boundary for tabula.read_pdf function
            today_day = today_date.day
            bottom = 145 + (today_day) * 10
            area = [140, 30, bottom, 540]

        else:

            # set the bottom boundary for tabula.read_pdf function for February months
            if date_structure.month == 2:
                area = [140, 30, 420, 540]

    elif report_type == 'kesdop':
        # set the default bottom boundary for tabula.read_pdf function
        area = [145, 30, 465, 881]

        # Set up a condition that replaces url with correct one each loop
        if date_structure.strftime('%Y-%m-%d') == today_date.strftime('%Y-%m-01'):
            # set the bottom boundary for tabula.read_pdf function
            today_day = today_date.day
            bottom = 150 + (today_day - 1) * 10
            area = [145, 45, bottom, 881]

        else:
            # set the bottom boundary for tabula.read_pdf function for February months
            if date_structure.month == 2:
                area = [145, 30, 443, 881]

    elif report_type == 'shadop':
        # set the default bottom boundary for tabula.read_pdf function
        area = [140, 30, 460, 540]

        # Set up a condition that replaces url with correct one each loop
        if date_structure.strftime('%Y-%m-%d') == today_date.strftime('%Y-%m-01'):

            # set the bottom boundary for tabula.read_pdf function
            today_day = today_date.day
            bottom = 140 + (today_day) * 10
            area = [145, 45, bottom, 540]
        else:

            # set the bottom boundary for tabula.read_pdf function for February months
            if date_structure.month == 2:
                area = [140, 30, 435, 540]

    elif report_type == 'doutdly':

        # if report is .txt extension
        if date_structure <= dt.datetime(2002, 4, 1):
            return None

        # if date is before including 2010 December, report is .prn extension
        elif dt.datetime(2002, 4, 1) < date_structure <= dt.datetime(2010, 12, 1):
            return None

        # provide pdf target area
        else:
            # dates of specific changes to pdf sizing
            if (date_structure.strftime('%Y-%m') == today_date.strftime('%Y-%m')
                    or (dt.datetime(2020, 1, 1) <= date_structure <= dt.datetime(2020, 8, 1)) 
                    or (dt.datetime(2019, 3, 1) <= date_structure <= dt.datetime(2019, 8, 1)) 
                    or (dt.datetime(2022, 6, 1) <= date_structure <= today_date)):
                area = [290.19, 20.76, 750.78, 1300.67]
                
            elif dt.datetime(2010, 12, 1) < date_structure <= dt.datetime(2017, 1, 1):
                # Weird date where pdf gets slightly longer
                # Other PDFs are smaller than the usual size
                if date_structure == dt.datetime(2011, 1, 1):
                    area = [146.19, 20.76, 350, 733.67]
                else:
                    area = [151.19, 20.76, 360, 900.67]
            elif date_structure == dt.datetime(2021, 12, 1):
                area = [290.19, 20.76, 1250.78, 1300.67]
            else:
                # area = [175.19, 20.76, 450.78, 900.67]
                area = [175.19, 20.76, 500.78, 900.67]

    return area


def get_date_published(url, date_structure, report_type):
    """
    Arguments:
        date_structure (datetime.datetime): report month/year represented as a datetime
        report_type(str): specifies the report table type
    Returns:
        dictionary: dictionary of data and metadata of report
    """
    date_published = None

    if url.endswith('.pdf'):

        if report_type == 'doutdly':

            # Dates of specific changes to pdf publish date sizing
            publish_date_target = [566, 566, 700, 800]
            today_date = dt.datetime.now()
            if (date_structure.strftime('%Y-%m') == today_date.strftime('%Y-%m')
                    or (dt.datetime(2020, 1, 1) <= date_structure <= dt.datetime(2020, 8, 1)) 
                    or (dt.datetime(2019, 3, 1) <= date_structure <= dt.datetime(2019, 8, 1)) 
                    or (dt.datetime(2022, 6, 1) <= date_structure <= today_date)):
                publish_date_target = [900, 850, 1200.78, 1400.67]

            pages = read_pdf(url, 
                             encoding='ISO-8859-1',
                             stream=True, 
                             area=publish_date_target, 
                             pages=1, 
                             pandas_options={'header': None})

            # check that a response is provided
            if len(pages) > 0:
                date_published = pages[0]
                if len(date_published.values) > 0:
                    date_published = dateutil.parser.parse(date_published.values[0][0]).date()

        # all others
        else:
            pages = read_pdf(url, 
                             encoding='ISO-8859-1',
                             stream=True, 
                             area=[10, 0, 13, 100], 
                             relative_area=True,
                             pages=1, 
                             pandas_options={'header': None})

            # check that a response is provided
            if len(pages) > 0:
                if len(pages[0].values) > 0:
                    date_published = dateutil.parser.parse(pages[0].values.tolist()[-1][-1]).date()

    # alernate report formats
    elif url.endswith('.prn') or url.endswith('.txt'):
        content = pd.read_fwf(url, nrows=1).columns[0]
        if content.startswith('Unnamed'):
            return None
        date_published = dateutil.parser.parse(pd.read_fwf(url, nrows=1).columns[0]).date()

    return date_published


def get_report(date_structure, report_type):
    """
    Arguments:
        date_structure (datetime.datetime): report month/year represented as a datetime
        report_type(str): specifies the report table type
    Returns:
        dictionary: dictionary of data and metadata of report
    """
    if date_structure.day != 1:
        print('WARNING: `date_structure` must represent start of report month; converting day value to 1.')
        date_structure = date_structure.replace(day=1)

    # construct report url
    url = get_url(date_structure, report_type)
    if url.endswith('.pdf'):

        # using the url, read pdf with tabula based off area coordinates
        content = read_pdf(url, 
                           encoding='ISO-8859-1',
                           stream=True, 
                           area=get_pdf_area(date_structure, report_type), 
                           pages=1, 
                           guess=False, 
                           pandas_options={'header': None})
        
        df = content[0] if isinstance(content, list) else content
        tail = df.tail(3)
        df.drop(tail.index, inplace=True)

    elif url.endswith('.prn'):
        df = pd.read_table(url, 
                           skiprows=10, 
                           # skipfooter=2,
                           names=get_report_columns(report_type, date_structure),
                           index_col=False,
                           delim_whitespace=True,
                           engine='python')

        tail = df['Date'].tail(2)
        df.drop(tail.index, inplace=True)
        # for key, row in full_data.iterrows():
        #     if len(row) > 23:
        #         print(row)
                    # day = re.split('(\d{3},\d{3})', row[19], maxsplit=1)[1:]
                    # full_data.loc[key, 19:] = row.shift(1) 
                    # full_data.loc[key, 19:20] = day[0], day[1]
                # if len(row[('outflow_index', 'outflow_7_dy_avg')]) > 10: 
                    # day = re.split('(\d{3},\d{3})', row[('outflow_index', 'outflow_7_dy_avg')], maxsplit=1)[1:]
                    # full_data.loc[key, ('outflow_index', 'outflow_7_dy_avg'):] = row.shift(1) 
                    # full_data.loc[key, ('outflow_index', 'outflow_7_dy_avg'): ('outflow_index', 'outflow_mnth_avg')] = day[0], day[1]
        # except pd.errors.EmptyDataError:
        #     pass

    elif url.endswith('.txt'):
        df = pd.read_csv(url, 
                         skiprows=10, 
                         sep=r'\s{1,}', 
                         index_col=False, 
                         names=get_report_columns(report_type, date_structure))#[:-2]

        tail = df['Date'].tail(2)
        df.drop(tail.index, inplace=True)

    # create date-indexed dataframe and convert numeric values to floats
    return {'data': data_cleaner(df, report_type, date_structure), 
            'info': {'url': url,
                     'title': get_title(report_type),
                     'date_published': get_date_published(url, date_structure, report_type),
                     'date_retrieved': dt.datetime.now()}}


def get_data(start, end, report_type):
    """
    earliest PDF date: Feburary 2000

    Arguments:
        start (datetime): start date given in datetime format
        end (datetime): end date given in datetime format
        report_type(str): specifies the reservoir used
    Returns:
        dictionary: dictionary of data and metadata of report
    """
    # check if date is in the right format
    assert isinstance(start, dt.datetime), 'Please give in datetime.datetime format'
    assert isinstance(end, dt.datetime), 'Please give in datetime.datetime format'
    assert start <= end, 'Specify date range where start <= end'

    # provide metadata for the specified query
    info = {'urls': [],
            'title': get_title(report_type),
            'dates_published': [],
            'date_retrieved': dt.datetime.now()}

    # loop through months in specified window
    frames = []
    for date_structure in months_between(start, end):

        # extract report content for month/year
        report = get_report(date_structure, report_type)

        # append report-specific info to query result metadata
        info['urls'].append(report['info']['url'])
        info['dates_published'].append(report['info']['date_published'])

        # append dataframes for each month
        frames.append(report['data'])

    # concatenate and set index for all appended dataframes
    df = pd.concat(frames).sort_index().truncate(before=start, after=end)

    # clean data and convert to multi-level columns
    return {'data': df, 'info': info}


def download_the_files(start, end, report_type):
    """
    Arguments:
        start (datetime.datetime):  start date given by user input
        end (datetime.datetime): end date given by user input
        report_type (str): the str identifier for CVO report
    Returns:
        None
    """
    for date_structure in months_between(start, end):
        response = requests.get(get_url(date_structure.strftime('%m%y'), report_type))
        with open('pdfs/{report_type}{date_structure:%m%y}.pdf', 'wb') as f:
            f.write(response.content)


# ========++++++============

def load_pdf_to_dataframe(ls, date_structure, report_type, to_csv=False):
    """
    changes dataframe to an array and reshape it column names change to what is specified below

    Arguments:
        ls (dataframe): dataframe in a [1, rows, columns] structure
        report_type (str): name of report
    Returns:
        df (pandas.DataFrame):
    """
    # remove all commas in number formatting
    df = ls[0].replace(',', '', regex=True)

    # filter so that only Day rows are included
    df = df.loc[df[0].astype(str).apply(lambda x: str(x).split()[0].isnumeric())]

    # remove any "NaN" entries for cases where offset created in parsing fixed-width columns
    df = pd.DataFrame(data=[list(map(float, row.split()[1:])) for row in df.to_string().replace('NaN', '').splitlines()[1:]])

    # update the column names
    df.columns = pd.MultiIndex.from_tuples(get_report_columns(report_type, date_structure))

    if report_type == 'dout':
        for key, value in df.iteritems():
            value = value.astype(str)
            value = value.replace(to_replace=r'[,\/]', value='', regex =True)
            value = value.replace(to_replace=r'[%\/]', value='', regex =True)
            # value = value.replace(to_replace=r'[(\/]', value='', regex =True)
            # value = value.replace(to_replace=r'[)\/]', value='', regex =True)
            df.loc[:, key] = value.astype(float)

    df = df.dropna(how='all').reindex()

    # change the dates in dataframe to date objects
    df['Date'] = df['Day'].astype(int).apply(
        lambda x: dt.datetime(date_structure.year, date_structure.month, x))

    # check that length of dataframe matches expected number of days
    assert (df.shape[0] == calendar.monthrange(date_structure.year,
        date_structure.month)[1] or df.shape[0] + 1 == dt.date.today().day), f'ASSERTION ERROR: {date_structure:%Y-%m}'

    # optionally write the month report data to CSV file
    if to_csv:
        df.to_csv(f'{date_structure:slunit_%b%Y}.csv')

    # return the report content
    return df


def data_cleaner(df, report_type):
    """
    This function converts data from string to floats and removes any non-numeric elements 

    Arguements:
        dataframe that was retreieved from file_getter function

    Returns:
        dataframe of strings converted to floats for data analysis
    """
    for key, value in df.iteritems():
        value = value.astype(str)
        value = value.replace(to_replace=r'[,\/]', value='', regex =True)
        value = value.replace(to_replace=r'[%\/]', value='', regex =True)

        # if report_type == 'dout':
        #     value = value.replace(to_replace=r'[(\/]', value='', regex =True)
        #     value = value.replace(to_replace=r'[)\/]', value='', regex =True)

        df.loc[:, key] = value.astype(float)

    # df.columns = pd.MultiIndex.from_tuples(get_report_columns(report_type, date_structure))
    return df


def get_area(date_structure, report_type):
    """
    set the default target area boundaries for tabula read_pdf function
    """
    if report_type == 'slunit':

        # set the default bottom boundary for tabula read_pdf function
        days_in_month = calendar.monthrange(date_structure.year, date_structure.month)[1]
        area = {28: [140, 30, 480, 700],
                29: [140, 30, 500, 700],
                30: [140, 30, 500, 700],
                31: [140, 30, 520, 700]}.get(days_in_month, [140, 30, 520, 700])

        if dt.datetime(date_structure.year, date_structure.month, 1) == dt.datetime(2000, 9, 1):
            area[2] = 520

        if dt.datetime(date_structure.year, date_structure.month, 1) == dt.datetime(2001, 6, 1):
            area[2] = 520

        if dt.datetime(date_structure.year, date_structure.month, 1) == dt.datetime(2001, 9, 1):
            area[2] = 520

        if dt.datetime(date_structure.year, date_structure.month, 1) == dt.datetime(2001, 11, 1):
            area[2] = 520

        if dt.datetime(date_structure.year, date_structure.month, 1) >= dt.datetime(2002, 3, 1):
            area = [140, 30, 600, 600]

        if dt.datetime(date_structure.year, date_structure.month, 1) > dt.datetime(2002, 8, 1):
            area[1] = 20

        if dt.datetime(date_structure.year, date_structure.month, 1) > dt.datetime(2012, 5, 1):

            area = {28: [120, 20, 440, 820],
                    29: [120, 20, 440, 820],
                    30: [120, 20, 460, 820],
                    31: [120, 20, 480, 820]}.get(days_in_month, [110, 30, 480, 820])

        if dt.datetime(date_structure.year, date_structure.month, 1) >= dt.datetime(2016, 11, 1):

            area = {28: [130, 0, 455, 900],
                    29: [130, 0, 480, 900],
                    30: [130, 0, 490, 900],
                    31: [130, 0, 490, 900]}.get(days_in_month, [130, 0, 490, 900])

        if dt.datetime(date_structure.year, date_structure.month, 1) ==  dt.datetime(2016, 12, 1):

            area = {28: [130, 0, 455, 900],
                    29: [130, 0, 480, 900],
                    30: [130, 0, 480, 900],
                    31: [130, 0, 490, 900]}.get(days_in_month, [130, 0, 490, 900])

        if dt.datetime(date_structure.year, date_structure.month, 1) ==  dt.datetime(2020, 2, 1):

            area = {28: [130, 0, 455, 900],
                    29: [130, 0, 460, 900],
                    30: [130, 0, 480, 900],
                    31: [130, 0, 490, 900]}.get(days_in_month, [130, 0, 490, 900])

        # # Set up a condition that replaces url with correct one each loop
        # if date_structure.strftime('%Y-%m-%d') == dt.datetime.now().strftime('%Y-%m-01'):

        #     # set the bottom boundary for tabula read_pdf function
        #     today_day = dt.datetime.now().day
        #     bottom = 145 + (today_day) * 10
        #     area = [140, 30, bottom, 640]

        return area

    if report_type == 'kesdop':

        # set the default bottom boundary for tabula read_pdf function
        area = [145, 30, 465, 881]

        if date_structure.month == 2:
            area = [145, 30, 443, 881]

        today = dt.date.today()

        # Set up a condition that replaces url with correct one each loop
        if date_structure.strftime('%Y-%m-%d') == today.strftime('%Y-%m-01'):
            bottom = 150 + (today.day - 1) * 10
            area = [145, 45, bottom, 881]
        
        if date_structure >= dt.date(2022, 9, 1):
            area = [145, 30, 700, 881]

        return area

    if report_type == 'fedslu':

        # set the default bottom boundary for tabula read_pdf function
        area = [140, 30, 500, 640]

        days_in_month = calendar.monthrange(date_structure.year, date_structure.month)[1]

        area = {28: [140, 30, 480, 700],
                29: [140, 30, 500, 700],
                30: [140, 30, 500, 700],
                31: [140, 30, 500, 700]}.get(days_in_month, [140, 30, 500, 700])

        # Set up a condition that replaces url with correct one each loop
        if date_structure.strftime('%Y-%m-%d') == today_date.strftime('%Y-%m-01'):
            url = 'https://www.usbr.gov/mp/cvo/vungvari/fedslu.pdf'
            urls.append(url)

            # set the bottom boundary for tabula read_pdf function
            today_day = today_date.day
            bottom = 145 + (today_day) * 10
            area = [140, 30, bottom, 640]

        else:
            # Using the list of dates, grab a url for each date
            url = get_url(date_structure, report_type)
            urls.append(url)

        #     # set the bottom boundary for tabula read_pdf function for February months
        #     if date_structure.month == 2:
        #         area = [140, 30, 600, 640]

        # if date_structure.year >= 2022 or date_structure.year == 2021 and date_structure.month == 11:
        return area

    if report_type == 'shadop':
        # set the default bottom boundary for tabula read_pdf function
        area = [140, 30, 460, 540]

        # set the bottom boundary for tabula read_pdf function for February months
        if date_structure.month == 2:
            area = [140, 30, 435, 540]

        if date_structure.strftime('%Y-%m-%d') == today_date.strftime('%Y-%m-01'):
            today_day = today_date.day
            bottom = 140 + (today_day) * 10
            area = [145, 45, bottom, 540]

        return area

    if report_type == 'shafln':

        # set the default bottom boundary for tabula read_pdf function
        area = [140, 30, 445, 540]

        if date_structure.month == 2:
            area = [140, 30, 420, 540]

        if date_structure.strftime('%Y-%m-%d') == today_date.strftime('%Y-%m-01'):

            # set the bottom boundary for tabula read_pdf function
            today_day = today_date.day
            bottom = 145 + (today_day) * 10
            area = [140, 30, bottom, 540]

        if date_structure.year >= 2022 or date_structure.year == 2021 and date_structure.month == 11:
            
            days_in_month = calendar.monthrange(date_structure.year, date_structure.month)[1]

            area = {28: [140, 30, 580, 700],
                    29: [140, 30, 600, 700],
                    30: [140, 30, 615, 700],
                    31: [140, 30, 630, 700]}.get(days_in_month, [140, 30, 600, 700])
        return area

    elif report_type == 'doutdly' or report_type == 'dout':

        # if report is .txt extension
        if date_structure <= dt.datetime(2002, 4, 1):
            return None

        # if date is before including 2010 December, report is .prn extension
        elif dt.datetime(2002, 4, 1) < date_structure <= dt.datetime(2010, 12, 1):
            return None

        # provide pdf target area
        else:
            # dates of specific changes to pdf sizing
            if (date_structure.strftime('%Y-%m') == today_date.strftime('%Y-%m')
                    or (dt.datetime(2020, 1, 1) <= date_structure <= dt.datetime(2020, 8, 1)) 
                    or (dt.datetime(2019, 3, 1) <= date_structure <= dt.datetime(2019, 8, 1)) 
                    or (dt.datetime(2022, 6, 1) <= date_structure <= today_date)):
                area = [290.19, 20.76, 750.78, 1300.67]
                
            elif dt.datetime(2010, 12, 1) < date_structure <= dt.datetime(2017, 1, 1):
                # Weird date where pdf gets slightly longer
                # Other PDFs are smaller than the usual size
                if date_structure == dt.datetime(2011, 1, 1):
                    area = [146.19, 20.76, 350, 733.67]
                else:
                    area = [151.19, 20.76, 360, 900.67]
            elif date_structure == dt.datetime(2021, 12, 1):
                area = [290.19, 20.76, 1250.78, 1300.67]
            else:
                # area = [175.19, 20.76, 450.78, 900.67]
                area = [175.19, 20.76, 500.78, 900.67]
        return area

    return area


def get_month_data(date_structure, report_type):
    """
    Arguments:
        date_structure (datetime.datetime): report date
        report_type (str): specifies the CVO report type used
    Returns:
        (tuple): paired dataframe and url for report data
    """
    # set the default boundary for tabula read_pdf function
    area = get_area(date_structure, report_type)

    # define the report URL for report type and date
    if os.path.exists(f'pdfs/{report_type}%m%y.pdf'):
        url = date_structure.strftime(f'pdfs/{report_type}%m%y.pdf')
    else:
        url = get_url(date_structure, report_type)

    # using the url, read pdf based off area coordinates
    # try:
        # append dataframes for each month
    df = load_pdf_to_dataframe(read_pdf(url,
                                            stream=True,
                                            area=area,
                                            pages=1,
                                            guess=False,
                                            pandas_options={'header': None}),
                                   date_structure,
                                   report_type)

    return df, url
        # print(f'APPEND, {date_structure:%Y %b}')

    # except:
    #     print(f'ERROR, {date_structure:%Y %b}')

    return pd.DataFrame(columns=['Date']), url


def get_data(start, end, report_type):
    """
    Arguments:
        start (datetime.datetime): start date given in datetime format
        end (datetime.datetime): end date given in datetime format
        report_type (str): specifies the CVO report type used
    Returns:
        result (dict): dictionary of data and metadata of report
    """
    # check if date is in the right format
    assert isinstance(start, dt.datetime), 'ERROR: provide start as datetime.datetime'
    assert isinstance(end, dt.datetime), 'ERROR: provide end as datetime.datetime'

    # loop through all months in defined date range
    frames, urls = zip(*[get_month_data(date, report_type) for date in months_between(start, end)])

    # concatenate and set index for all appended dataframes
    df = pd.concat(frames).set_index('Date').truncate(before=start, after=end) 

    # clean data and convert to multi-level columns
    result = data_cleaner(df, report_type)

    # reindex for continuous record
    result = result.reindex(pd.date_range(start=result.first_valid_index(),
                                          end=result.last_valid_index(),
                                          freq='D'))

    # result
    return {'data': result, 
            'info': {'url': urls,
                     'title': get_title(report_type),
                     'date retrieved': dt.datetime.now().__str__()}}


def file_getter_dout(start, end, report_type='dout'):
    # Check if date is in the right format
    validate_user_date(start)
    validate_user_date(end)

    today_date = dt.datetime.now()

    # Defining variables
    frames = []
    urls = []
    dates_published = []

    # Dates of specific changes to pdf sizing
    small_pdf = dt.datetime.strptime('0117', '%m%y')
    prn_date = dt.datetime.strptime('1210', '%m%y')
    txt_date = dt.datetime.strptime('0402', '%m%y')
    special_date = dt.datetime.strptime('0111', '%m%y')
    blown_up_start1 = dt.datetime.strptime('0120', '%m%y')
    blown_up_end1 = dt.datetime.strptime('0820', '%m%y')
    blown_up_start2 = dt.datetime.strptime('0319', '%m%y')
    blown_up_end2 = dt.datetime.strptime('0819', '%m%y')
    blown_up_start3 = dt.datetime.strptime('0622', '%m%y')

    column_names = [
        'Date',
        'SactoR_pd',
        'SRTP_pd',
        'Yolo_pd',
        'East_side_stream_pd',
        'Joaquin_pd',
        'Joaquin_7dy',
        'Joaquin_mth',
        'total_delta_inflow',
        'NDCU',
        'CLT',
        'TRA',
        'CCC',
        'BBID',
        'NBA',
        'total_delta_exports',
        '3_dy_avg_TRA_CLT',
        'NDOI_daily',
        'outflow_7_dy_avg',
        'outflow_mnth_avg',
        'exf_inf_daily',
        'exf_inf_3dy',
        'exf_inf_14dy'
    ]

    # Getting list of dates for url
    for dt_month in months_between(start, end):
        dt_month = dt.datetime.strptime(dt_month.strftime('%m%y'), '%m%y')

        url = get_url(dt_month.strftime('%m%y'), report_type)
        urls.append(url)

        if dt_month.strftime('%Y-%m') == today_date.strftime('%Y-%m'):
            url = 'https://www.usbr.gov/mp/cvo/vungvari/doutdly.pdf'
            full_data = read_pdf(url,
                                 encoding='ISO-8859-1',
                                 stream=True,
                                 area=[290.19, 20.76,750.78 ,1300.67],
                                 pages=1,
                                 guess=False,
                                 pandas_options={'header':None})
            date_published = read_pdf(url,
                                 encoding='ISO-8859-1',
                                 stream=True,
                                 area=[900, 850,1200.78 ,1400.67],
                                 pages=1,
                                 pandas_options={'header':None})
            urls[-1] = url

        elif ((blown_up_start1 <= dt_month <= blown_up_end1)
                or (blown_up_start2 <= dt_month <= blown_up_end2) 
                or (blown_up_start3 <= dt_month <= today_date)):
            full_data = read_pdf(url,
                                 encoding='ISO-8859-1',
                                 stream=True,
                                 area=[290.19, 20.76,750.78 ,1300.67],
                                 pages=1,
                                 guess=False,
                                 pandas_options={'header':None})
            date_published = read_pdf(url,
                                 encoding='ISO-8859-1',
                                 stream=True,
                                 area=[900, 850,1200.78 ,1400.67],
                                 pages=1,
                                 pandas_options={'header':None})

        elif prn_date < dt_month <= small_pdf:
            # Weird date where pdf gets slightly longer
            # Other PDFs are smaller than the usual size
            if dt_month == special_date:
                full_data = read_pdf('https://www.usbr.gov/mp/cvo/vungvari/dout0111.pdf',
                                     encoding='ISO-8859-1',
                                     stream=True,
                                     area=[146.19, 20.76, 350, 733.67],
                                     pages=1,
                                     guess=False,
                                     pandas_options={'header':None})
            else:
                full_data = read_pdf(url,
                                     encoding='ISO-8859-1',
                                     stream=True,
                                     area=[151.19, 20.76, 360, 900.67],
                                     pages=1,
                                     guess=False,
                                     pandas_options={'header':None})
            date_published = read_pdf(url,
                                 encoding='ISO-8859-1',
                                 stream=True,
                                 area=[566, 566, 700, 800],
                                 pages=1,
                                 pandas_options={'header':None})

        elif txt_date < dt_month <= prn_date:
            try:
            # prn file format
                time = pd.read_fwf(url,index_col = False,infer_nrows = 0)
                date_published = [time.columns]

                full_data = pd.read_table(url, skiprows=10, skipfooter=2,
                    names=column_names,
                    index_col=False,
                    delim_whitespace=True)

                for key, row in full_data.iterrows():
                    if len(row['outflow_7_dy_avg']) > 10:
                        day = re.split('(\d{3},\d{3})',
                                        row['outflow_7_dy_avg'],
                                        maxsplit=1)[1:]
                        full_data.loc[key,'outflow_7_dy_avg':] = row.shift(1)
                        full_data.loc[key,'outflow_7_dy_avg':'outflow_mnth_avg'] = day[0], day[1]

            except pd.errors.EmptyDataError:
                pass

        elif dt_month <= txt_date:
            # txt file format
            time = pd.read_fwf(url, index_col=False, infer_nrows=False)
            date_published = [time.columns]
            txt = pd.read_csv(url, skiprows=10, sep=r'\s{1,}', index_col=False, names=column_names)
            full_data = txt[:-2]

        else:
            full_data = read_pdf(url,
                                 encoding='ISO-8859-1',
                                 stream=True,
                                 area=[175.19, 20.76, 450.78, 900.67],
                                 pages=1,
                                 guess=False,
                                 pandas_options={'header':None})
            date_published = read_pdf(url,
                                 encoding='ISO-8859-1',
                                 stream=True,
                                 area=[566, 566, 700, 800],
                                 pages=1,
                                 pandas_options={'header':None})
            full_data = full_data[0]

        pdf_time= pd.to_datetime(date_published[0][0])
        dates_published.append(pdf_time)

        pdf_df = load_pdf_to_dataframe(full_data, dt_month, report_type)
        pdf_df['Date'] = pdf_df['Date'].apply(lambda x: dateutil.parser.parse(x))
        frames.append(pdf_df)

    df = pd.concat(frames).set_index('Date').sort_index().truncate(before=start, after=end)

    return {'data': data_cleaner(df, report_type),
            'info': {'url': urls,
                     'title': get_title(report_type)}}
