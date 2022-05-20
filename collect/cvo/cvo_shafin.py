"""
collect.cvo.cvo_shafin
============================================================
access cvo data
"""
# -*- coding: utf-8 -*-
import datetime
from datetime import date 
from pandas.core.indexes.datetimes import date_range

import pandas as pd
import numpy as np
import tabula 
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
      # Check if date is in the right format
    validate(start)
    validate(end)

    today_date = date.today()
    today_month = int(today_date.strftime('%m'))

    # Defining variables
    date_list = []
    urls = []
    result = pd.DataFrame()
    current_month = 'https://www.usbr.gov/mp/cvo/vungvari/shafln.pdf'

	# Getting list of dates for url
    for month in months_between(start, end):
        dates = month.strftime("%m%y")
        date_list.append(dates)


	# Using the list of dates, grab a url for each date
    for date in date_list:
        url = url_maker(date,'shafin')
        urls.append(url)

	# Since the current month url is slightly different, we set up a condition that replaces that url with the correct one
    if today_month == int(end.strftime('%m')):
        urls[-1] = current_month

	# Using the url, grab the pdf and concatenate it based off dates
    count = 0
    for links in urls:
		# Finding out if it is in feburary or not
        month = links[-8:-6]
        if month == '02':
            pdf1 = read_pdf(links,
		        stream=True, area = [140, 30,420,881], pages = 1, guess = False,  pandas_options={'header':None})
        
        elif links == current_month:
            today_day = int(today_date.strftime('%d'))
            bottom = 150 + (today_day-1)*10

            pdf1 = read_pdf(links,
		        stream=True, 
                area = [140, 30,bottom,881], 
                pages = 1, guess = False,  pandas_options={'header':None})

        
        else:
            pdf1 = read_pdf(links,
		        stream=True, area = [140, 30,440,881], pages = 1, guess = False,  pandas_options={'header':None})
                
        pdf_df = df_generator(pdf1,'shafin')

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

    new_df = data_cleaner(new_df,'shafin')

    tuples = (('Storage_AF','britton'),('Storage_AF','mccloud'),('Storage_AF','iron_canyon'),('Storage_AF','pit6'),('Storage_AF','pit7'),
    ('Res','res_total'),
    ('change','d_af'),('change','d_cfs'),
    ('Shasta_inflow','shasta_inf'),
    ('Nat_river','nat_river'),
    ('accum_full_1000af','accum_full_1000af'))

    new_df.columns = pd.MultiIndex.from_tuples(tuples)

    return new_df
	#return dates


