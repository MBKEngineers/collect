"""
collect.cvo.cvo_shafln
============================================================
access cvo data
"""
# -*- coding: utf-8 -*-
import datetime
from datetime import date 

import pandas as pd
from tabula import read_pdf

from collect.cvo.cvo_common import report_type_maker, months_between, load_pdf_to_dataframe, validate_user_date, data_cleaner


# input as a range of dates
# Takes range of dates and uses report_type_maker to get multiple pdfs
# Format: 'YYYY/MM/DD'
def file_getter_shafln(start, end):
    """
    Earliest PDF date: Feburary 2000

    Arguements:
        Range of dates including start and end month
        Given in YYYY/MM/DD format and as a datetime object

    Returns:
        dataframe of date range 

    """
    # Check if date is in the right format
    validate_user_date(start)
    validate_user_date(end)

    today_date = date.today()

    # Defining variables
    dates_published = list(months_between(start, end))
    frames = []
    urls = []

	# Getting list of dates for url
    for dt_month in dates_published:
        
        # set the default bottom boundary for tabula read_pdf function
        area = [140, 30,445,540]

        # Set up a condition that replaces url with correct one each loop
        if dt_month.strftime('%Y-%m-%d') == today_date.strftime('%Y-%m-01'):
            url = 'https://www.usbr.gov/mp/cvo/vungvari/shafln.pdf'
            urls.append(url)

            # set the bottom boundary for tabula read_pdf function
            today_day = today_date.day
            bottom = 145 + (today_day)*10
            Area = [140, 30,bottom,540]

        else:
            # Using the list of dates, grab a url for each date
            url = report_type_maker(dt_month.strftime('%m%y'), 'shafln')
            urls.append(url)

            # set the bottom boundary for tabula read_pdf function for February months
            if dt_month.month == 2:
                area = [140, 30, 420, 540]

        # using the url, read pdf based off area coordinates
        pdf1 = read_pdf(url, 
                        stream=True, 
                        area = area, 
                        pages = 1, 
                        guess = False, 
                        pandas_options={'header':None})
        pdf_df = load_pdf_to_dataframe(pdf1,'shafln')

        # change the dates in pdf_df to date objects
        pdf_df['Date'] = pdf_df['Date'].apply(lambda x: datetime.date(dt_month.year, dt_month.month, x))

        # append dataframes for each month
        frames.append(pdf_df)

    # 
    df = pd.concat(frames).set_index('Date').truncate(before=start, after=end) 

    new_df = data_cleaner(df,'shafln')

        # tuple format: (top, bottom)

    tuples = (("Storage_AF","britton"),("Storage_AF","mccloud"),("Storage_AF","iron_canyon"),
                ("Storage_AF","pit6"),("Storage_AF","pit7"),
                ("Res","res_total"),
                ("change","d_af"),("change","d_cfs"),
                ("Shasta_inflow","shasta_inf"),
                ("Nat_river","nat_river"),
                ("accum_full_1000af","accum_full_1000af"))
    new_df.columns = pd.MultiIndex.from_tuples(tuples)

    print(new_df.head())

    return {'data': new_df, 'info': {'url': urls,
                                 'title': "Shasta Reservoir Daily Operations",
                                 'units': 'cfs',
                                 'date published': dates_published,
                                 'date retrieved': today_date}}

if __name__ == '__main__':

    start_date = datetime.date(2021,9,10)
    end_date = datetime.date.today()

    data = file_getter_shafln(start_date,end_date)
