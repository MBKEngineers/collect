"""
collect.cvo.cvo
==========================================================================
Functions that are used throughout the cvo scripts

Some will have multiple args to differentiate between the CVO data that is being read
"""
# -*- coding: utf-8 -*-
import datetime as dt
import dateutil.parser
import pandas as pd
import requests
try:
    from tabula import read_pdf
except:
    print('WARNING: tabula is required for cvo module')


def get_url(date_structure, report_type):
    """
    Arguments:
        date_structure (datetime.datetime): datetime representing report year and month
        report_type (str): one of 'kesdop', 'shadop', 'shafln', 'doutdly'
    Returns:
        url (str): the PDF resource URL for the specified month and report type
    """
    # current month URL
    if date_structure.strftime('%Y-%m') == dt.datetime.now().strftime('%Y-%m'):
        return f'https://www.usbr.gov/mp/cvo/vungvari/{report_type}.pdf'

    # special handling for delta outflow calculation reports
    elif report_type == 'doutdly':

        # if date is before including 2002 April, give the txt link
        if date_structure <= dt.datetime(2002, 4, 1):
            return f'https://www.usbr.gov/mp/cvo/vungvari/dout{date_structure:%m%y}.txt'

        # if date is before including 2010 December, give the prn link
        elif dt.datetime(2002, 4, 1) < date_structure <= dt.datetime(2010, 12, 1):
            return f'https://www.usbr.gov/mp/cvo/vungvari/dout{date_structure:%m%y}.prn'

        # if not, give in pdf format
        return f'https://www.usbr.gov/mp/cvo/vungvari/dout{date_structure:%m%y}.pdf'

    # default report URL for past months
    return f'https://www.usbr.gov/mp/cvo/vungvari/{report_type}{date_structure:%m%y}.pdf'


def months_between(start_date, end_date):
    """
    Given two instances of ``datetime.datetime``, generate a list of dates on
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
        yield dt.datetime(year, month, 1)

        # Move to the next month.  If we're at the end of the year, wrap around
        # to the start of the next.
        #
        # Example: Nov 2017
        #       -> Dec 2017 (month += 1)
        #       -> Jan 2018 (end of year, month = 1, year += 1)
        #
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

    elif report_type == 'shafln':
        tuples = (('Date', 'Date'), 
                  ('Storage_AF', 'britton'),
                  ('Storage_AF', 'mccloud'),
                  ('Storage_AF', 'iron_canyon'),
                  ('Storage_AF', 'pit6'),
                  ('Storage_AF', 'pit7'),
                  ('Res', 'res_total'),
                  ('change', 'd_af'),
                  ('change', 'd_cfs'),
                  ('Shasta_inflow', 'shasta_inf'),
                  ('Nat_river', 'nat_river'),
                  ('accum_full_1000af', 'accum_full_1000af'))

    elif report_type == 'kesdop':
        tuples = (('Date', 'Date'), 
                  ('Elevation', 'elev'),
                  ('Storage_AF', 'storage'),
                  ('Storage_AF', 'change'),
                  ('CFS', 'inflow'),
                  ('Spring_release', 'spring_release'),
                  ('Shasta_release', 'shasta_release'),
                  ('Release_CFS', 'power'),
                  ('Release_CFS', 'spill'),
                  ('Release_CFS', 'fishtrap'),
                  ('Evap_cfs', 'evap_cfs'))

    elif report_type == 'shadop':
        tuples = (('Date', 'Date'), 
                  ('Elevation', 'elev'),
                  ('Storage_1000AF', 'in_lake'),
                  ('Storage_1000AF', 'change'),
                  ('CFS', 'inflow_cfs'),
                  ('Release_CFS', 'power'),
                  ('Release_CFS', 'spill'),
                  ('Release_CFS', 'outlet'),
                  ('Evaporation', 'evap_cfs'),
                  ('Evaporation', 'evap_in'),
                  ('Precip_in', 'precip_in'))

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
