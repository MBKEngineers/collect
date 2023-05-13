"""
collect.cvo.cvo_slunit
============================================================
access cvo data

Example usage:
    result = file_getter(dt.datetime(2021, 4, 1), dt.datetime.now())
    print(result['data'].tail())
"""
# -*- coding: utf-8 -*-
import calendar
import datetime as dt

import pandas as pd
try:
    from tabula import read_pdf
except:
    print('WARNING: tabula is required for cvo module')
import requests

from collect.cvo.cvo_common import report_type_maker, months_between, load_pdf_to_dataframe, validate_user_date, data_cleaner

def get_area(dt_month):

    # set the default bottom boundary for tabula read_pdf function
    days_in_month = calendar.monthrange(dt_month.year, dt_month.month)[1]
    area = {28: [140, 30, 480, 700],
            29: [140, 30, 500, 700],
            30: [140, 30, 500, 700],
            31: [140, 30, 520, 700]}.get(days_in_month, [140, 30, 520, 700])

    if dt.datetime(dt_month.year, dt_month.month, 1) > dt.datetime(2002, 8, 1):
        area[1] = 20

    if dt.datetime(dt_month.year, dt_month.month, 1) > dt.datetime(2012, 5, 1):

        area = {28: [120, 20, 440, 820],
                29: [120, 20, 440, 820],
                30: [120, 20, 460, 820],
                31: [120, 20, 470, 820]}.get(days_in_month, [110, 30, 480, 820])

    if dt.datetime(dt_month.year, dt_month.month, 1) > dt.datetime(2016, 11, 1):

        area = {28: [130, 0, 455, 900],
                29: [130, 0, 480, 900],
                30: [130, 0, 480, 900],
                31: [130, 0, 490, 900]}.get(days_in_month, [130, 0, 490, 900])

    if dt.datetime(dt_month.year, dt_month.month, 1) ==  dt.datetime(2016, 12, 1):

        area = {28: [130, 0, 455, 900],
                29: [130, 0, 480, 900],
                30: [130, 0, 480, 900],
                31: [130, 0, 490, 900]}.get(days_in_month, [130, 0, 490, 900])

    if dt.datetime(dt_month.year, dt_month.month, 1) ==  dt.datetime(2020, 2, 1):

        area = {28: [130, 0, 455, 900],
                29: [130, 0, 460, 900],
                30: [130, 0, 480, 900],
                31: [130, 0, 490, 900]}.get(days_in_month, [130, 0, 490, 900])

    # Set up a condition that replaces url with correct one each loop
    if dt_month.strftime('%Y-%m-%d') == dt.datetime.now().strftime('%Y-%m-01'):

        # set the bottom boundary for tabula read_pdf function
        today_day = dt.datetime.now().day
        bottom = 145 + (today_day) * 10
        area = [140, 30, bottom, 640]

    return area


def file_getter(start, end, report_type='slunit'):
    """
    Earliest PDF date: Feburary 2000
    Range of dates including start and end month
    Given in YYYY/MM/DD format and as a datetime object

    Arguements:
        start (datetime.datetime): start date given in datetime format
        end (datetime.datetime): end date given in datetime format
        report_type (str): specifies the CVO report type used

    Returns:
        dictionary: dictionary of data and metadata of report

    """
    # Check if date is in the right format
    validate_user_date(start)
    validate_user_date(end)

    today_date = dt.datetime.now()

    # Defining variables
    dates_published = list(months_between(start, end))
    frames = []
    urls = []

    # Getting list of dates for url
    for dt_month in dates_published:

        # set the default boundary for tabula read_pdf function
        area = get_area(dt_month)

        # Set up a condition that replaces url with correct one each loop
        if dt_month.strftime('%Y-%m-%d') == today_date.strftime('%Y-%m-01'):
            url = 'https://www.usbr.gov/mp/cvo/vungvari/slunit.pdf'
            urls.append(url)

        else:
            # Using the list of dates, grab a url for each date
            # response = requests.get(report_type_maker(dt_month.strftime('%m%y'), report_type))
            # with open(dt_month.strftime('pdfs/slunit%m%y.pdf'), 'wb') as f:
            #     f.write(response.content)

            url = dt_month.strftime('pdfs/slunit%m%y.pdf')
            urls.append(url)

        # using the url, read pdf based off area coordinates
        # try:
        if True:
            pdf1 = read_pdf(url, 
                            stream=True, 
                            area=area, 
                            pages=1, 
                            guess=False, 
                            pandas_options={'header': None})

            pdf_df = load_pdf_to_dataframe(pdf1, report_type)

            # change the dates in pdf_df to date objects
            pdf_df['Date'] = pdf_df['__DAY'].astype(int).apply(lambda x: dt.datetime(dt_month.year, dt_month.month, x))

            pdf_df.to_csv(f'temp/{dt_month:slunit_%b%Y}.csv')

            # append dataframes for each month
            frames.append(pdf_df)

            print(f'APPEND, {dt_month:%Y %b}')

        # except:
        #     print(f'ERROR, {dt_month:%Y %b}')

    # concatenate and set index for all appended dataframes
    df = pd.concat(frames).set_index('Date').truncate(before=start, after=end) 

    # clean data and convert to multi-level columns
    new_df = data_cleaner(df, report_type)

    # result
    return {'data': new_df, 
            'info': {'url': urls,
                     'title': 'San Luis Reservoir Operations',
                     'units': 'cfs',
                     'date published': dates_published,
                     'date retrieved': today_date}}


def download_the_files():
    start = dt.datetime(2012, 6, 1)
    end = dt.datetime(2020, 12, 31)
    for dt_month in months_between(start, end):
        response = requests.get(report_type_maker(dt_month.strftime('%m%y'), 'slunit'))
        with open(dt_month.strftime('pdfs/slunit%m%y.pdf'), 'wb') as f:
            f.write(response.content)


if __name__ == '__main__':

    # result = file_getter(dt.datetime(2000, 1, 1), dt.datetime(2012, 5, 31))
    # result['data'].to_csv('temp/raw_slunit_Jan2000-May2012.csv')
    # result = file_getter(dt.datetime(2014, 1, 1), dt.datetime(2016, 11, 30))
    # result['data'].to_csv('temp/raw_slunit_Jan2014-Nov2016.csv')
    # result = file_getter(dt.datetime(2017, 1, 1), dt.datetime(2018, 1, 31))
    # result['data'].to_csv('temp/raw_slunit_Jan2017-Jan2018.csv')

    # result = file_getter(dt.datetime(2016, 11, 1), dt.datetime(2020, 12, 31))
    # result['data'].to_csv('temp/raw_slunit_Dec2016-Dec2020.csv')

    # result = file_getter(dt.datetime(2012, 6, 1), dt.datetime(2013, 12, 31))
    # result['data'].to_csv('temp/raw_slunit_Jun2012-Dec2013.csv')

    pass
