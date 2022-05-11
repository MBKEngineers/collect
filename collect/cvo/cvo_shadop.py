"""
collect.cvo.cvo_shadop
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



############# functions
# input as a string
# Takes month and year to create the url link to grab the PDF from
def url_maker(date):
	"""
    Arguments:
        date (str): the date but in MMYY format, ex 0520
    Returns:
        url (str): the url for requesting the file
        Specific to shadop data
    """
	text = "https://www.usbr.gov/mp/cvo/vungvari/shadop"
	final = text + date + ".pdf"

	return str(final)

def months_between(start_date, end_date):
    """
    Given two instances of ``datetime.date``, generate a list of dates on
    the 1st of every month between the two dates (inclusive).

    Inputs start and end date

    e.g. "5 Jan 2020" to "17 May 2020" would generate:

        1 Jan 2020, 1 Feb 2020, 1 Mar 2020, 1 Apr 2020, 1 May 2020

    """
    if start_date > end_date:
        raise ValueError(f"Start date {start_date} is not before end date {end_date}")

    year = start_date.year
    month = start_date.month

    while (year, month) <= (end_date.year, end_date.month):
        yield datetime.date(year, month, 1)

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

def df_generator_shadop(ls):
    """
    Arguements: 
        dataframe in a [1,rows,columns] structure
        Change to an array and reshape it

        Minor adjustment for shadop files
    Returns:
        dataframe in a [rows,columns] structure, changes column names to the correct ones

    Function is specific to shadop
    """
    ls = np.array(ls)
    ls1 = ls[0]

	# Change from array to dataframe, generate new columns
    df = pd.DataFrame(ls1,columns=[ "Date","elev","in_lake","change","inflow_cfs",
	"power", "spill", "outlet","evap_cfs","evap_in","precip_in"])


    return df

def validate(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y/%m/%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY/MM/DD")


# Making everything to a function
def data_cleaner(df):
    cols = df.columns
    n_rows = len(df)
    n_cols = len(cols)
	# Going through each cell to change the numbering format
	# ie going from 1,001 to 1001
	# Also converting from string to integer 
    df[cols] = df[cols].astype(str)

    for i in range(n_rows):
        for j in range(n_cols):
            df.iloc[i][j] = df.iloc[i][j].replace(',','') 
            df.iloc[i][j] = df.iloc[i][j].replace('%','')
    df[cols] = df[cols].astype(float)

    return df

# input as a range of dates
# Takes range of dates and uses url_maker to get multiple pdfs
# Format: 'YYYY/MM/DD'
def file_getter_shadop(start, end):
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
    current_month = 'https://www.usbr.gov/mp/cvo/vungvari/shadop.pdf'

	# Getting list of dates for url
    for month in months_between(start_date, end_date):
        dates = month.strftime("%m%y")
        foo.append(dates)


	# Using the list of dates, grab a url for each date
    for foos in foo:
        url = url_maker(foos)
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
		        stream=True, area = [140,30,435,881], pages = 1, guess = False,  pandas_options={'header':None})
        
        elif links == current_month:
            today_day = int(today_date.strftime('%d'))
            # today date-1 since the file does not include current date
            bottom = 150 + (today_day-1)*10
            pdf1 = read_pdf(links,
		        stream=True, 
                area = [145, 45,bottom,881], 
                pages = 1, guess = False,  pandas_options={'header':None})

        else:
            pdf1 = read_pdf(links,
		        stream=True, area = [140,30,460,881], pages = 1, guess = False,  pandas_options={'header':None})
        pdf_df = df_generator_shadop(pdf1)

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
	            'Storage_1000AF','Storage_1000AF',
	            'CFS',
	            'Release_CFS','Release_CFS','Release_CFS',
	            'Evaporation','Evaporation',
	            'Precip_in']
    bottom_level =["elev",
	               "in_lake","change",
	               "inflow_cfs",
	               "power", "spill", "outlet",
	               "evap_cfs","evap_in",
	               "precip_in"]

    arrays = [top_level,bottom_level]
    tuples = list(zip(*arrays))
    new_df.columns = pd.MultiIndex.from_tuples(tuples)

    return new_df
	#return dates

data = file_getter_shadop('2022/04/15','2022/05/11')

