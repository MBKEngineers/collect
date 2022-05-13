"""
collect.cvo.cvo_shafin
============================================================
access cvo data
"""
# -*- coding: utf-8 -*-
import io
import re
import datetime
from datetime import date 
from pandas.core.indexes.datetimes import date_range

import requests
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

    # Convert into string
    start = str(start)
    end = str(end)

	# Defining dates
    s_year,s_month,s_day = start.split("/")
    e_year,e_month,e_day = end.split("/")

    start_date = date(int(s_year), int(s_month), int(s_day))
    end_date = date(int(e_year), int(e_month), int(e_day))

    today_date = date.today()
    today_month = int(today_date.strftime('%m'))

    # Defining variables
    foo = []
    urls = []
    result = pd.DataFrame()
    current_month = 'https://www.usbr.gov/mp/cvo/vungvari/shafln.pdf'

	# Getting list of dates for url
    for month in months_between(start_date, end_date):
        dates = month.strftime("%m%y")
        foo.append(dates)


	# Using the list of dates, grab a url for each date
    for foos in foo:
        url = url_maker(foos,'shafin')
        urls.append(url)

	# Since the current month url is slightly different, we set up a condition that replaces that url with the correct one
    if today_month == int(e_month):
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

            correct_date = '20'+ foo[count][2:4] + '-' + foo[count][0:2] +'-'+ str(day) + ' '
            combined = correct_date + default_time
            datetime_object = datetime.datetime.strptime(combined, '%Y-%m-%d %H:%M:%S')
            correct_dates.append(datetime_object)

        pdf_df['Date'] = correct_dates
        result = pd.concat([result,pdf_df])
        count +=1

	# Extract date range 
    new_start_date = start_date.strftime("%Y-%m-%d")
    new_end_date = end_date.strftime("%Y-%m-%d")

    mask = (result['Date'] >= new_start_date) & (result['Date'] <= new_end_date)
    new_df = result.loc[mask]

	# Set DateTime Index
    new_df.set_index('Date', inplace = True)

    new_df = data_cleaner(new_df,'shafin')

    top_level = ['Storage_AF','Storage_AF','Storage_AF','Storage_AF','Storage_AF',
            'Res',
            'change','change',
            'Shasta_inflow',
            'Nat_river',
            'accum_full_1000af']
            
    bottom_level =["britton","mccloud","iron_canyon","pit6","pit7",
    "res_total", 
    "d_af","d_cfs",
    "shasta_inf",
    "nat_river",
    "accum_full_1000af"]

    arrays = [top_level,bottom_level]
    tuples = list(zip(*arrays))
    new_df.columns = pd.MultiIndex.from_tuples(tuples)

    return new_df
	#return dates


