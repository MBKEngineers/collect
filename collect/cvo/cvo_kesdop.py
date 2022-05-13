"""
collect.cvo.cvo_kesdop
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
    current_month = 'https://www.usbr.gov/mp/cvo/vungvari/kesdop.pdf'

	# Getting list of dates for url
    for month in months_between(start_date, end_date):
        dates = month.strftime("%m%y")
        foo.append(dates)


	# Using the list of dates, grab a url for each date
    for foos in foo:
        url= url_maker(foos)
        urls.append(url)

	# Since the current month url is slightly different, we set up a condition that replaces that url with the correct one
    if today_month == int(e_month):
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
                
        pdf_df = df_generator_kesdop(pdf1)

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

    new_df = data_cleaner(new_df)

    top_level = ['Elevation',
            'Storage_AF','Storage_AF',
            'CFS',
            'Spring_release',
            'Shasta_release',
            'Release_CFS','Release_CFS','Release_CFS',
            'Evap_cfs']
    bottom_level =["elev",
    "storage","change",
    "inflow",
    "spring_release", 
    "shasta_release", 
    "power","spill","fishtrap",
    "evap_cfs"]

    arrays = [top_level,bottom_level]
    tuples = list(zip(*arrays))
    new_df.columns = pd.MultiIndex.from_tuples(tuples)

    return new_df
	#return dates

data = file_getter_kesdop('2019/04/15','2022/05/11')

