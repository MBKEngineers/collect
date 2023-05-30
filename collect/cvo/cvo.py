"""
collect.cvo.cvo
==========================================================================
functions to support retrieving tabular data from CVO monthly report PDFs
"""
# -*- coding: utf-8 -*-
import calendar
import datetime as dt
import os

from colors import color
import dateutil.parser
import pandas as pd
import requests
try:
    from tabula import read_pdf
except:
    print('WARNING: tabula is required for cvo module')


REPORTS = [
    'doutdly',
    'kesdop',
    'fedslu',
    'shadop',
    'shafln',
    'slunit',
    'trndop',
    'nmlfln',
    'lewdop',
    'nmldop',
    'whidop',
    'tuldop',
    'sprdop',
    'gdwdop',
    'mildop',
    'milfln',
    'shafcr',
    'epkdop',
    'stgdop',
    'foldop',
    'berdop',
    'natdop',
    'soldop',
    'snldop',
    'cchdop',
    'onfdop',
]

SUPPORTED_REPORTS = [
    'doutdly',
    'kesdop',
    'fedslu',
    'shadop',
    'shafln',
    'slunit',
]


def get_area(date_structure, report_type):
    """
    set the default target area boundaries for tabula read_pdf function

    Arguments:
        date_structure (datetime.date): report month/year represented as a datetime
        report_type(str): specifies the report table type
    Returns:
        area (list): list of target area dimensions in order of: top, left, bottom, right
    """
    today_day = dt.date.today().day
    report_date = dt.date(date_structure.year, date_structure.month, 1)

    if report_type == 'doutdly':

        # if date is before including 2010 December, report is .txt or .prn extension
        if report_date <= dt.date(2010, 12, 1):
            return None

        # provide pdf target area
        # dates of specific changes to pdf sizing
        if (report_date.strftime('%Y-%m') == dt.date.today().strftime('%Y-%m')
                or (dt.date(2020, 1, 1) <= report_date <= dt.date(2020, 8, 1))
                or (dt.date(2019, 3, 1) <= report_date <= dt.date(2019, 8, 1))
                or (dt.date(2022, 6, 1) <= report_date <= dt.date.today())):
            area = [290.19, 20.76, 750.78, 1300.67]

        elif dt.date(2010, 12, 1) < report_date <= dt.date(2017, 1, 1):
            # Weird date where pdf gets slightly longer
            # Other PDFs are smaller than the usual size
            if report_date == dt.date(2011, 1, 1):
                area = [146.19, 20.76, 350, 733.67]

            elif report_date in [dt.date(2013, 8, 1), dt.date(2013, 10, 1)]:
                area = [151.19, 20.76, 380, 900.67]

            elif report_date == dt.date(2013, 12, 1):
                area = [151.19, 20.76, 390, 900.67]

            else:
                area = [151.19, 20.76, 360, 900.67]

        elif report_date == dt.datetime(2021, 12, 1):
            area = [290.19, 20.76, 1250.78, 1300.67]

        else:
            area = [175.19, 20.76, 500.78, 900.67]

    if report_type == 'fedslu':

        # set the default bottom boundary for tabula read_pdf function
        area = [140, 30, 500, 640]

        days_in_month = calendar.monthrange(date_structure.year, date_structure.month)[1]

        area = {28: [140, 30, 480, 700],
                29: [140, 30, 500, 700],
                30: [140, 30, 500, 700],
                31: [140, 30, 500, 700]}.get(days_in_month, [140, 30, 500, 700])

        # Set up a condition that replaces url with correct one each loop
        if date_structure.strftime('%Y-%m-%d') == dt.date.today().strftime('%Y-%m-01'):

            # set the bottom boundary for tabula read_pdf function
            today_day = dt.date.today().day
            bottom = 145 + (today_day) * 10
            area = [140, 30, bottom, 640]

        #     # set the bottom boundary for tabula read_pdf function for February months
        #     if date_structure.month == 2:
        #         area = [140, 30, 600, 640]

        # if date_structure.year >= 2022 or date_structure.year == 2021 and date_structure.month == 11:
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

    if report_type == 'shadop':
        # set the default bottom boundary for tabula read_pdf function
        area = [140, 30, 460, 540]

        # set the bottom boundary for tabula read_pdf function for February months
        if date_structure.month == 2:
            area = [140, 30, 435, 540]

        if date_structure.strftime('%Y-%m') == dt.date.today().strftime('%Y-%m'):
            bottom = 140 + (today_day) * 10
            area = [145, 45, bottom, 540]

        return area

    if report_type == 'shafln':

        # set the default bottom boundary for tabula read_pdf function
        area = [140, 30, 445, 540]

        if date_structure.month == 2:
            area = [140, 30, 420, 540]

        if date_structure.strftime('%Y-%m-%d') == dt.date.today().strftime('%Y-%m-01'):

            # set the bottom boundary for tabula read_pdf function
            bottom = 145 + (today_day) * 10
            area = [140, 30, bottom, 540]

        if date_structure.year >= 2022 or date_structure.year == 2021 and date_structure.month == 11:
            days_in_month = calendar.monthrange(date_structure.year, date_structure.month)[1]

            area = {28: [140, 30, 580, 700],
                    29: [140, 30, 600, 700],
                    30: [140, 30, 615, 700],
                    31: [140, 30, 630, 700]}.get(days_in_month, [140, 30, 600, 700])
        return area

    if report_type == 'slunit':

        # set the default bottom boundary for tabula read_pdf function
        days_in_month = calendar.monthrange(date_structure.year, date_structure.month)[1]
        area = {28: [140, 30, 480, 700],
                29: [140, 30, 500, 700],
                30: [140, 30, 500, 700],
                31: [140, 30, 520, 700]}.get(days_in_month, [140, 30, 520, 700])

        if report_date in [dt.date(2000, 9, 1),
                           dt.date(2001, 6, 1),
                           dt.date(2001, 9, 1),
                           dt.date(2001, 11, 1)]:
            area[2] = 520

        if report_date >= dt.date(2002, 3, 1):
            area = [140, 30, 600, 600]

        if report_date > dt.date(2002, 8, 1):
            area[1] = 20

        if report_date > dt.date(2012, 5, 1):
            area = {28: [120, 20, 440, 820],
                    29: [120, 20, 440, 820],
                    30: [120, 20, 460, 820],
                    31: [120, 20, 480, 820]}.get(days_in_month, [110, 30, 480, 820])

        if report_date == dt.date(2014, 1, 1):
            area = [130, 20, 490, 820]

        if report_date == dt.date(2014, 2, 1):
            area = [130, 20, 455, 820]

        if report_date >= dt.date(2014, 3, 1):
            area = [130, 20, 490, 820]

        if report_date >= dt.date(2016, 11, 1):
            area = {28: [130, 0, 455, 900],
                    29: [130, 0, 480, 900],
                    30: [130, 0, 490, 900],
                    31: [130, 0, 490, 900]}.get(days_in_month, [130, 0, 490, 900])

        if report_date ==  dt.date(2016, 12, 1):
            area = {28: [130, 0, 455, 900],
                    29: [130, 0, 480, 900],
                    30: [130, 0, 480, 900],
                    31: [130, 0, 490, 900]}.get(days_in_month, [130, 0, 490, 900])

        if report_date ==  dt.date(2020, 2, 1):
            area = {28: [130, 0, 455, 900],
                    29: [130, 0, 460, 900],
                    30: [130, 0, 480, 900],
                    31: [130, 0, 490, 900]}.get(days_in_month, [130, 0, 490, 900])

        # set the bottom boundary for tabula read_pdf function for current month
        if report_date == dt.date.today().replace(day=1):
            area = [140, 30, 145 + (today_day) * 10, 640]

        return area

    return area


def get_data(start, end, report_type):
    """
    retrieve CVO report data spanning multiple months, as specified by query date range

    Arguments:
        start (datetime.date): start date given in datetime format
        end (datetime.date): end date given in datetime format
        report_type (str): specifies the CVO report type
    Returns:
        result (dict): dictionary of data and metadata of report
    """
    # optionally convert start, end to date objects
    start = start.date() if isinstance(start, dt.datetime) else start
    end = end.date() if isinstance(end, dt.datetime) else end

    # check query parameters
    assert start > dt.date(2000, 1, 1), 'ERROR: CVO library begins in February 2000'
    assert isinstance(start, dt.date), 'ERROR: provide start as datetime.date'
    assert isinstance(end, dt.date), 'ERROR: provide end as datetime.date'
    assert start <= end, 'ERROR: specify date range where start <= end'

    # report and query metadata
    info = {'urls': [],
            'title': get_title(report_type),
            'dates published': [],
            'date retrieved': dt.datetime.now()}

    # loop through all months in defined date range
    frames = []
    for date_structure in months_between(start, end):

        try:
            # extract report content for month/year
            report = get_report(date_structure, report_type)

            # append report-specific info to query result metadata
            info['urls'].append(report['info']['url'])
            # info['dates published'].append(report['info']['date published'])

            # append dataframes for each month
            frames.append(report['data'])

            print(color(f'SUCCESS: {report_type} {date_structure:%b %Y}', 'cyan'))

        except:
            print(color(f'ERROR: {report_type} {date_structure:%b %Y}', 'red'))

    # concatenate and set index for all appended dataframes
    df = pd.concat(frames, axis=0)

    # reindex for continuous record
    df = df.reindex(pd.date_range(start=df.first_valid_index(),
                                  end=df.last_valid_index(),
                                  freq='D'))

    # truncate result to query range; return timeseries data and report metadata
    return {'data': df.sort_index().truncate(before=start, after=end),
            'info': info}


def get_date_published(url, date_structure, report_type):
    """
    Arguments:
        date_structure (datetime.date): report month/year represented as a datetime
        report_type(str): specifies the report table type
    Returns:
        date_published (datetime.date): the extracted date of report
    """
    date_published = None

    if url.endswith('.pdf'):

        # delta daily outflow report
        if report_type == 'doutdly':

            # Dates of specific changes to pdf publish date sizing
            publish_date_target = [566, 566, 700, 800]
            today_date = dt.date.today()
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


def get_report_columns(report_type, date_structure, expected_length=None, default=False):
    """
    Arguments:
        report_type (str):
        date_structure (datetime.date):
        expected_length (int): expected length of columns (axis=1)
        default (bool): flag to indicate which set of tuples should be used as default
    Returns:
        (tuple): tuple of tuples representing multi-level column names
    Raises:
        NotImplementedError: raises error if invalid report_type is supplied
    """
    if report_type == 'doutdly':
        tuples = (
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

    elif report_type == 'fedslu':
        tuples = (
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

    elif report_type == 'kesdop':
        tuples = (
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
        tuples = (
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

    elif report_type == 'shafln':
        tuples = (
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

    # note: original report has an additional layer of numbered columns for reports starting in June 2012
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
            *(('DOS AMIGOS', 'DOS AMIGOS', 'XVC') if (date_structure > dt.date(2013, 12, 31) or default) else ()),
            ('DOS AMIGOS', 'DOS AMIGOS', 'FED'),
            ('DOS AMIGOS', 'DOS AMIGOS', 'STATE'),
            ('DOS AMIGOS', 'DOS AMIGOS', 'TOTAL'),
        ]
        # spelling 'AQEDUCT' adopted directly from report
        if date_structure >= dt.date(2012, 6, 1) or default:
            tuples += [
                ('AQEDUCT CHECK 21', 'AQEDUCT CHECK 21', 'FED'),
                ('AQEDUCT CHECK 21', 'AQEDUCT CHECK 21', 'STATE'),
                ('AQEDUCT CHECK 21', 'AQEDUCT CHECK 21', 'TOTAL'),
            ]
        return tuples

    else:
        raise NotImplementedError(f'report_type {report_type} is not supported.')

    if isinstance(expected_length, int):
        return tuples[:expected_length]
    return tuples


def get_report(date_structure, report_type):
    """
    get report content for one month for the specified report_type

    Arguments:
        date_structure (datetime.date): report month/year represented as a python datetime.date
        report_type(str): specifies the report table type
    Returns:
        dictionary: dictionary of data and metadata of report
    """
    if date_structure.day != 1:
        print('WARNING: `date_structure` must represent start of report month; converting day value to 1.')
        date_structure = date_structure.replace(day=1)

    # construct report url
    url = get_url(date_structure, report_type)
    # if os.path.exists(f'pdfs/{report_type}/' + url.split('/')[-1]):
    #     url = f'pdfs/{report_type}/' + url.split('/')[-1]

    # using the url, read pdf with tabula based off area coordinates
    if url.endswith('.pdf'):
        content = read_pdf(url,
                           encoding='ISO-8859-1',
                           stream=True,
                           area=get_area(date_structure, report_type),
                           pages=1,
                           guess=False,
                           pandas_options={'header': None})
        if report_type == 'doutdly':
            df = doutdly_data_cleaner(content, report_type, date_structure)
        else:
            df = load_pdf_to_dataframe(content, date_structure, report_type)

    if url.endswith('.prn'):
        df = pd.read_table(url,
                           skiprows=10,
                           names=get_report_columns(report_type, date_structure),
                           index_col=False,
                           delim_whitespace=True,
                           engine='python')
        df = doutdly_data_cleaner(df, report_type, date_structure)

    elif url.endswith('.txt'):
        df = pd.read_csv(url,
                         skiprows=10,
                         sep=r'\s{1,}',
                         index_col=False,
                         names=get_report_columns(report_type, date_structure))
        df = doutdly_data_cleaner(df, report_type, date_structure)

    # create date-indexed dataframe and convert numeric values to floats
    return {'data': df,
            'info': {'url': url,
                     'title': get_title(report_type),
                     # 'date_published': get_date_published(url, date_structure, report_type),
                     'date_retrieved': dt.datetime.now()}}


def get_title(report_type):
    """
    get the title for the identified CVO report

    Arguments:
        report_type (str): one of 'kesdop', 'shadop', 'shafln', 'doutdly', 'fedslu', 'slunit'
    Returns:
        title (str): the report title text
    """
    return {
        'doutdly': 'U.S. Bureau of Reclamation - Central Valley Operations Office Delta Outflow Computation',
        'fedslu': 'San Luis Reservoir Federal Daily Operations',
        'kesdop': 'Kesdop Reservoir Daily Operations',
        'shadop': 'Shadop Reservoir Daily Operations',
        'shafln': 'Shasta Reservoir Daily Operations',
        'slunit': 'Federal-State Operations, San Luis Unit',
    }.get(report_type, '')


def get_url(date_structure, report_type):
    """
    construct the query URL for specified year/month and report type

    Arguments:
        date_structure (datetime.date): date representing report year and month
        report_type (str): one of SUPPORTED_REPORTS
    Returns:
        url (str): the PDF resource URL for the specified month and report type
    """
    # current month URL
    if date_structure.strftime('%Y-%m') == dt.date.today().strftime('%Y-%m'):
        return f'https://www.usbr.gov/mp/cvo/vungvari/{report_type}.pdf'

    # special handling for delta outflow calculation reports
    if report_type == 'doutdly':

        # if date is less than or equal to April 2002, use txt format
        if date_structure <= dt.date(2002, 4, 1):
            return f'https://www.usbr.gov/mp/cvo/vungvari/dout{date_structure:%m%y}.txt'

        # if date is less than or equal to December 2010, use prn format
        if date_structure <= dt.date(2011, 3, 1):
            return f'https://www.usbr.gov/mp/cvo/vungvari/dout{date_structure:%m%y}.prn'

        # reference pdf format
        return f'https://www.usbr.gov/mp/cvo/vungvari/dout{date_structure:%m%y}.pdf'

    # default report URL for past months
    return f'https://www.usbr.gov/mp/cvo/vungvari/{report_type}{date_structure:%m%y}.pdf'


def months_between(start_date, end_date):
    """
    given two instances of ``datetime.date``, generate a list of dates on
    the 1st of every month between the two dates (inclusive).

    Arguments:
        start_date (datetime.date):  start date given by user input
        end_date (datetime.date): end date given by user input
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


def doutdly_data_cleaner(content, report_type, date_structure):
    """
    This function converts data from string to floats and removes any non-numeric elements

    Arguments:
        content (dataframe): daily data as a dataframe
        report_type (string): report type identifier
        date_structure (datetime.datetime): date of the monthly report
    Returns:
        df (dataframe): in a [rows,columns] structure,
    """
    content = content[0] if isinstance(content, list) else content

    # Change from array to dataframe, generate new columns
    df = pd.DataFrame(content if content.ndim <= 2 else content[0])

    # remove any "NaN" entries for cases where offset created in parsing fixed-width columns
    df = pd.DataFrame(data=[row.split()[1:] for row in df.to_string().replace('NaN', '').splitlines()[1:]])

    # set the multi-level column names
    df.columns = pd.MultiIndex.from_tuples(get_report_columns(report_type,
                                                              date_structure,
                                                              expected_length=len(df.columns)))

    # reindex to match the version of the report with the most columns, for concatenating full record requests
    header = pd.MultiIndex.from_tuples(get_report_columns(report_type, date_structure))
    df = df.reindex(header, axis=1)

    # change the dates in dataframe to date objects (if represented as integer days, construct
    # date with current month/year)
    df = df.set_index(('Date', 'Date'))
    df.index.name = None

    # drop non-date index entries
    df = df.loc[~df.index.isna()]

    df.index = df.index.map(lambda x: dt.date(date_structure.year, date_structure.month, x) if str(x).isnumeric()
                                      else dateutil.parser.parse(x) if x != '-' and str(x[0]).isnumeric()
                                      else None)

    # drop non-date index entries
    df = df.loc[~df.index.isna()]

    # typo in Dec 2007 .prn file results in assigment of year 2000
    df.index = [x.replace(year=date_structure.year) for x in df.index]

    # convert numeric data to floats, including parentheses notation to negative numbers
    df = (df.replace(',', '', regex=True)
            .replace('%', '', regex=True)
            .replace('None', float('nan'), regex=True)
            .replace(r'[\$,)]', '', regex=True)
            .replace(r'[(]', '-', regex=True)
            .astype(float))

    # drop COA columns with no data
    if 'COA USBR' in df:
        if df['COA USBR']['Account Balance'].dropna().empty:
            df.drop('COA USBR', axis=1, inplace=True)

    # return converted dataframe; drop NaN values
    return df.dropna(how='all').reindex()


def load_pdf_to_dataframe(content, date_structure, report_type, to_csv=False):
    """
    changes dataframe to an array and reshape it column names change to what is specified below

    Arguments:
        content (dataframe): dataframe in a [1, rows, columns] structure
        report_type (str): name of report
    Returns:
        df (pandas.DataFrame):
    """
    # remove all commas in number formatting
    df = content[0].replace(',', '', regex=True)

    # remove rows with NaN in Day column
    df = df.loc[~df[0].isna()]

    # filter so that only numeric Day rows are included
    df = df.loc[df[0].astype(str).str.match(r'\d+(\.\d+)?'), :]

    # remove any "NaN" entries for cases where offset created in parsing fixed-width columns
    df = pd.DataFrame(data=[list(map(float, row.split()[1:]))
                            for row in df.to_string().replace('NaN', '').splitlines()[1:]])

    # update the column names
    df.columns = pd.MultiIndex.from_tuples(get_report_columns(report_type, date_structure))

    # reindex to match the version of the report with the most columns, for concatenating full record requests
    header = pd.MultiIndex.from_tuples(get_report_columns(report_type, date_structure, default=True))
    df = df.reindex(header, axis=1)

    # data cleaning specific to delta outflow report
    if report_type == 'doutdly':
        # convert numeric data to floats, including parentheses notation to negative numbers
        df = (df.replace(',', '', regex=True)
                .replace('%', '', regex=True)
                .replace('None', float('nan'), regex=True)
                .replace(r'[\$,)]', '', regex=True)
                .replace(r'[(]', '-', regex=True)
                .astype(float))

    # drop any rows where all values are missing
    df = df.dropna(how='all').reindex()

    # change the dates in dataframe to date objects
    df['Date'] = df['Day'].astype(int).apply(lambda x: dt.date(date_structure.year, date_structure.month, x))

    # set the DateIndex
    df = df.set_index('Date')
    df = df.sort_index()

    # check that length of dataframe matches expected number of days
    message = f'ERROR: row count does not match number of days in {date_structure:%b %Y}'
    assert (df.shape[0] == calendar.monthrange(date_structure.year, date_structure.month)[1]
            or df.shape[0] + 1 == dt.date.today().day), message

    # optionally write the month report data to CSV file
    if to_csv:
        df.to_csv(f'{date_structure:slunit_%b%Y}.csv')

    # return the report content
    return df


def download_files(start, end, report_type, destination='.'):
    """
    download file contents for all reports within date range to specified destination directory

    Arguments:
        start (datetime.date):  start date given by user input
        end (datetime.date): end date given by user input
        report_type (str): the str identifier for CVO report
        destination (str): destination path for saving report files
    Returns:
        None
    """
    if not os.path.exists(destination):
        os.makedirs(destination)

    for date_structure in months_between(start, end):
        url = get_url(date_structure, report_type)
        response = requests.get(url)

        with open(os.path.join(destination, url.split('/')[-1]), 'wb') as f:
            f.write(response.content)
