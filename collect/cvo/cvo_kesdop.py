"""
collect.cvo.cvo_kesdop
============================================================
access cvo data
"""
# -*- coding: utf-8 -*-
import datetime
from datetime import date 
from pandas.core.indexes.datetimes import date_range

import pandas as pd
import numpy as np
from tabula import read_pdf

from collect.cvo.cvo_common import url_maker, months_between, df_generator, validate, data_cleaner

# input as a range of dates
# Takes range of dates and uses url_maker to get multiple pdfs
# Format: 'YYYY/MM/DD'
def file_getter_kesdop(start, end):
    """
    Arguements:
        range of dates including start and end month
        Given in YYYY/MM/DD format

    Returns:
        dataframe of date range 

    """
    today_date = date.today()
    today_month = int(today_date.strftime('%m'))

    # Defining variables
    date_list = []
    urls = []
    result = pd.DataFrame()
    current_month = 'https://www.usbr.gov/mp/cvo/vungvari/kesdop.pdf'

	# Getting list of dates for url
    for month in months_between(start, end):
        dates = month.strftime("%m%y")
        date_list.append(dates)


	# Using the list of dates, grab a url for each date
    for date_url in date_list:
        url = url_maker(date_url,'kesdop')
        urls.append(url)

	# Since the current month url is slightly different, we set up a condition that replaces that url with the correct one
    if today_month == int(end.strftime('%m')):
        urls[-1] = current_month

	# Using the url, grab the pdf and concatenate it based off dates
    count = 0
    for links in urls:
        month = links[-8:-6]
		# Finding out if it is in feburary or not
        if month == '02':
            pdf1 = read_pdf(links,
		        stream=True, area = [145, 30,443,881], pages = 1, guess = False,  pandas_options={'header':None})
        elif links == current_month:
            today_day = int(today_date.strftime('%d'))
            bottom = 150 + today_day*10

            pdf1 = read_pdf(links,
		        stream=True, 
                area = [145, 45,bottom,881], 
                pages = 1, guess = False,  pandas_options={'header':None})

        else:
            pdf1 = read_pdf(links,
		        stream=True, area = [145, 30,465,881 ], pages = 1, guess = False,  pandas_options={'header':None})
                
        pdf_df = df_generator(pdf1,'kesdop')

		# change the dates in pdf_df to datetime
        default_time = '00:00:00'
        correct_dates = []
        for i in range(len(pdf_df['Date'])):
            day = pdf_df['Date'][i]
            day = str(day)

            if len(day) !=2:
                day = '0' + day
                pdf_df['Date'][i] = day
            else:
                pass

            correct_date = '20'+ date_list[count][2:4] + '-' + date_list[count][0:2] +'-'+ str(day) + ' '
            combined = correct_date + default_time
            datetime_object = datetime.datetime.strptime(combined, '%Y-%m-%d %H:%M:%S')
            correct_dates.append(datetime_object)

        pdf_df['Date'] = correct_dates
        result = pd.concat([result,pdf_df])
        count +=1

	# Extract date range 
    new_start_date = start.strftime("%Y-%m-%d")
    new_end_date = end.strftime("%Y-%m-%d")

    mask = (result['Date'] >= new_start_date) & (result['Date'] <= new_end_date)
    new_df = result.loc[mask]

	# Set DateTime Index
    new_df.set_index('Date', inplace = True)

    new_df = data_cleaner(new_df,'kesdop')
    # tuple format: (top, bottom)
    tuples = (('Elevation','elev'),
    ('Storage_AF','storage'),('Storage_AF','change'),
    ('CFS','inflow'),
    ('Spring_release','spring_release'),
    ('Shasta_release','shasta_release'),
    ('Release_CFS','power'),('Release_CFS','spill'),('Release_CFS','fishtrap'),
    ('Evap_cfs','evap_cfs'))

    new_df.columns = pd.MultiIndex.from_tuples(tuples)
    print(new_df.head())

    return {'data': new_df, 'info': {'url': urls,
                                 'title': "Keswick Reservoir Daily Operations",
                                 'units': 'cfs',
                                 'date published': today_date}}
	#return dates


if __name__ == '__main__':
    start_date = datetime.datetime(2021,1,10)
    end_date = datetime.datetime(2022,4,20)
    data = file_getter_kesdop(start_date,end_date)