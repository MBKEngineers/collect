"""
collect.cvo.cvo_shafln
============================================================
access cvo data

Example usage:
    result = file_getter_shafln(dt.datetime(2021, 4, 1), dt.datetime.now())
    print(result['data'].tail())
"""
# -*- coding: utf-8 -*-
import datetime as dt

import pandas as pd
try:
    from tabula import read_pdf
except:
    print('WARNING: tabula is require for cvo module')

from collect.cvo.cvo_common import report_type_maker, months_between, load_pdf_to_dataframe, validate_user_date, data_cleaner


def file_getter_shafln(start, end, report_type='shafln'):
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
        area = [140, 30, 445, 540]

        # Set up a condition that replaces url with correct one each loop
        if dt_month.strftime('%Y-%m-%d') == today_date.strftime('%Y-%m-01'):
            url = 'https://www.usbr.gov/mp/cvo/vungvari/shafln.pdf'
            urls.append(url)

            # set the bottom boundary for tabula read_pdf function
            today_day = today_date.day
            bottom = 145 + (today_day)*10
            area = [140, 30, bottom, 540]

        else:
            # Using the list of dates, grab a url for each date
            url = report_type_maker(dt_month.strftime('%m%y'), report_type)
            urls.append(url)

            # set the bottom boundary for tabula read_pdf function for February months
            if dt_month.month == 2:
                area = [140, 30, 420, 540]

        if dt_month.year >= 2022:
            area[-1] = 700

        # using the url, read pdf based off area coordinates
        try:
            pdf1 = read_pdf(url, 
                            stream=True, 
                            area=area, 
                            pages=1, 
                            guess=False, 
                            pandas_options={'header': None})
            pdf_df = load_pdf_to_dataframe(pdf1, report_type)

            # change the dates in pdf_df to date objects
            pdf_df['Date'] = pdf_df['Date'].apply(lambda x: dt.datetime(dt_month.year, dt_month.month, x))

            # append dataframes for each month
            frames.append(pdf_df)
        except:

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

    result = file_getter_shafln(dt.datetime(2000, 2, 1), dt.datetime(2023, 5, 1))
    print(result['data'].tail())

