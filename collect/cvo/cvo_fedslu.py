"""
collect.cvo.cvo_fedslu
============================================================
access cvo data

Example usage:
    result = file_getter_fedslu(dt.datetime(2021, 4, 1), dt.datetime.now())
    print(result['data'].tail())
"""
# -*- coding: utf-8 -*-
import calendar
import datetime as dt

import pandas as pd
try:
    from tabula import read_pdf
except:
    print('WARNING: tabula is require for cvo module')

from collect.cvo.cvo_common import report_type_maker, months_between, load_pdf_to_dataframe, validate_user_date, data_cleaner


def file_getter_fedslu(start, end, report_type='fedslu'):
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

        # set the default bottom boundary for tabula read_pdf function
        area = [140, 30, 500, 640]

        days_in_month = calendar.monthrange(dt_month.year, dt_month.month)[1]

        area = {28: [140, 30, 480, 700],
                29: [140, 30, 500, 700],
                30: [140, 30, 500, 700],
                31: [140, 30, 500, 700]}.get(days_in_month, [140, 30, 500, 700])


        # Set up a condition that replaces url with correct one each loop
        if dt_month.strftime('%Y-%m-%d') == today_date.strftime('%Y-%m-01'):
            url = 'https://www.usbr.gov/mp/cvo/vungvari/fedslu.pdf'
            urls.append(url)

            # set the bottom boundary for tabula read_pdf function
            today_day = today_date.day
            bottom = 145 + (today_day) * 10
            area = [140, 30, bottom, 640]

        else:
            # Using the list of dates, grab a url for each date
            url = report_type_maker(dt_month.strftime('%m%y'), report_type)
            urls.append(url)

        #     # set the bottom boundary for tabula read_pdf function for February months
        #     if dt_month.month == 2:
        #         area = [140, 30, 600, 640]

        # if dt_month.year >= 2022 or dt_month.year == 2021 and dt_month.month == 11:
            

        # using the url, read pdf based off area coordinates
        try:
            pdf1 = read_pdf(url, 
                            stream=True, 
                            area=area, 
                            pages=1, 
                            guess=False, 
                            pandas_options={'header': None})

            pdf_df = load_pdf_to_dataframe(pdf1, report_type)
            pdf_df = pdf_df.loc[pdf_df['Day'].astype(str).str.isnumeric()]

            # change the dates in pdf_df to date objects
            pdf_df['Date'] = pdf_df['Day'].astype(int).apply(lambda x: dt.datetime(dt_month.year, dt_month.month, x))

            pdf_df.to_csv(f'temp/{dt_month:fedslu_%b%Y}.csv')

            # append dataframes for each month
            frames.append(pdf_df)

        except:
            print(f'ERROR, {dt_month:%Y %b}')

    # concatenate and set index for all appended dataframes
    df = pd.concat(frames).set_index('Date').truncate(before=start, after=end) 

    # clean data and convert to multi-level columns
    new_df = data_cleaner(df, report_type)

    # result
    return {'data': new_df, 
            'info': {'url': urls,
                     'title': 'Shasta Reservoir Daily Operations',
                     'units': 'cfs',
                     'date published': dates_published,
                     'date retrieved': today_date}}


if __name__ == '__main__':

    result = file_getter_fedslu(dt.datetime(2020, 6, 1), dt.datetime(2021, 1, 1))
    result['data'].to_csv('temp/por_fedslu_2.csv')

