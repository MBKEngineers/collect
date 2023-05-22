"""
collect.cvo.common
==========================================================================
Functions that are used throughout the cvo scripts

Some will have multiple args to differentiate between the CVO data that is being read
"""
# -*- coding: utf-8 -*-
import calendar
import datetime as dt
import os

import pandas as pd
from tabula import read_pdf


def get_title(report_type):
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


def get_url(date, report_type):
    """
    Arguments:
        date (str): the date but in MMYY format, ex 0520
        report_type (str): specify which pdf you're looking at
    Returns:
        url (str): the url for requesting the file
    """
    current_month = date == dt.date.today().strftime('%m%y')

    if report_type == 'dout':
        prn = dt.date(2010, 12, 1)
        txt = dt.date(2002, 4, 1)
        file_date = dt.date(2000 + int(date[2:4]), int(date[0:2]), 1)
        
        # current month has a different report title
        if current_month:
            return f'https://www.usbr.gov/mp/cvo/vungvari/doutdly.pdf' 

        # if date is before including 2010 December, give the prn link
        if txt < file_date <= prn:
            return f'https://www.usbr.gov/mp/cvo/vungvari/{report_type}{date}.prn'

        # if date is before including 2002 April, give the txt link
        elif file_date <= txt:
            return f'https://www.usbr.gov/mp/cvo/vungvari/{report_type}{date}.txt'

    if current_month:
        return f'https://www.usbr.gov/mp/cvo/vungvari/{report_type}.pdf'

    return f'https://www.usbr.gov/mp/cvo/vungvari/{report_type}{date}.pdf'


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
        yield dt.date(year, month, 1)

        # increment date by one month
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1
    

def get_column_names(date, report_type):
    # establishing column names
    return {

        'shadop': ['Date', 'elev', 'in_lake', 'change', 'inflow_cfs', 
                   'power', 'spill', 'outlet', 'evap_cfs', 'evap_in', 
                   'precip_in'],

        'shafln': ['Date', 'britton', 'mccloud', 'iron_canyon', 'pit6',
                   'pit7', 'res_total', 'd_af', 'd_cfs', 'shasta_inf', 
                   'nat_river', 'accum_full_1000af'],

        'dout': ['Date', 'SactoR_pd', 'SRTP_pd', 'Yolo_pd', 'East_side_stream_pd', 
                 'Joaquin_pd', 'Joaquin_7dy','Joaquin_mth', 'total_delta_inflow', 
                 'NDCU', 'CLT', 'TRA', 'CCC', 'BBID', 'NBA', 'total_delta_exports', 
                 '3_dy_avg_TRA_CLT', 'NDOI_daily','outflow_7_dy_avg', 
                 'outflow_mnth_avg', 'exf_inf_daily','exf_inf_3dy', 'exf_inf_14dy'],

        'fedslu':  ['Day', 'Elev', 'Storage', 'Change', 
                    'Federal Pump', 'Federal Gen', 'Pacheco Pump', 'ADJ', 
                    'Federal Change', 'Federal Storage'],


    }.get(report_type)


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
    df.columns = pd.MultiIndex.from_tuples(get_column_tuples(date_structure, report_type))

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


def validate_user_date(date_text):
    """
    Checks if users date is in valid format
    Arguments:
        date_text (user input): date of user input
    Returns:
        None: if user input is not datetime the process will end
    """

    #if condition returns True, then nothing happens:
    assert isinstance(date_text, dt.datetime), 'Please give in datetime format'


def get_column_tuples(date, report_type):
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
            ('Export/Inflow', '14 Day (%)')
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

    # df.columns = pd.MultiIndex.from_tuples(get_column_tuples(date_structure, report_type))
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
            url = get_url(date_structure.strftime('%m%y'), report_type)
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
        url = get_url(date_structure.strftime('%m%y'), report_type)

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
    validate_user_date(start)
    validate_user_date(end)

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
