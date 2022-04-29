"""
collect.cvo.cvo_dout
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


# input as a string
# Takes month and year to create the url link to grab the PDF from
def url_maker_doutdly(date):
    """
    Arguments:
        date (str): the date but in MMYY format, ex 0520
    Returns:
        url (str): the url for requesting the file
        if date is before including 2011 march, give the prn link
        if not give in pdf format
    """

    # construct the station url
    change = datetime.date(2010, 12, 1)
    bar = datetime.date(2000+int(date[2:4]), int(date[0:2]),1)

    if bar <= change:
        text = "https://www.usbr.gov/mp/cvo/vungvari/dout"
        final = text + date + ".prn"
    else:
        text = "https://www.usbr.gov/mp/cvo/vungvari/dout"
        final = text + date + ".pdf"


    return str(final)

# inputs start and end date that is in datetime format
def months_between(start_date, end_date):
    """
    Given two instances of ``datetime.date``, generate a list of dates on
    the 1st of every month between the two dates (inclusive).

    e.g. "5 Jan 2020" to "17 May 2020" would generate:

        1 Jan 2020, 1 Feb 2020, 1 Mar 2020, 1 Apr 2020, 1 May 2020

    """
    if start_date > end_date:
        raise ValueError("Start date {start_date} is not before end date {end_date}")

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



def df_generator(ls):
    """
    Arguements: 
        dataframe in a [1,rows,columns] structure
        Change to an array and reshape it

        Minor adjustment for dout files
    Returns:
        dataframe in a [rows,columns] structure, changes column names to the correct ones

    Function is specific to dout
    """
    ls = np.array(ls)
    ls1 = ls[0]

    # Change from array to dataframe, generate new columns
    df = pd.DataFrame(ls1,columns=["Date", "SactoR_pd","SRTP_pd", "Yolo_pd","East_side_stream_pd","Joaquin_pd","Joaquin_7dy","Joaquin_mth", "total_delta_inflow",
    "NDCU", "CLT","TRA","CCC","BBID","NBA","total_delta_exports","3_dy_avg_TRA_CLT","NDOI_daily","outflow_7_dy_avg","outflow_mnth_avg","exf_inf_daily",
    "exf_inf_3dy","exf_inf_14dy"])

    df = df.dropna()
    df = df.reindex()
    return df

def validate(date_text):
    """
    Arguements:
        date in datetime format
    Returns:
        Checks if date is in the correct format of YYYY/MM/DD
    """
    try:
        datetime.datetime.strptime(date_text, '%Y/%m/%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY/MM/DD")

def data_cleaner(df):
    """
    Arguements:
        dataframe that was retreieved from file_getter function

    Returns:
        dataframe of strings converted to floats for data analysis
    """
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

    # Hardcoding it for the last 3 columns since we know that it is in percentages
    df.iloc[:,-3:] = df.iloc[:,-3:]/100
    return df

def file_getter_dout(start, end):
    """
    Arguements:
        range of dates including start and end month
        Given in YYYY/MM/DD format

    Returns:
        dataframe of date range 

    """
    # Check if date is in the right format
    # validate(start)
    # validate(end)

    # Convert into string
    start = str(start)
    end = str(end)

    # Defining dates
    s_year,s_month,s_day = start.split("/")
    e_year,e_month,e_day = end.split("/")

    start_date = datetime.date(int(s_year), int(s_month), int(s_day))
    end_date = datetime.date(int(e_year), int(e_month), int(e_day))

    today_date = date.today()
    today_month = int(today_date.strftime('%m'))
    today_year = int(today_date.strftime('%Y'))

    # Defining variables
    foo = []
    foo_dtime = []
    urls = []
    result = pd.DataFrame()
    current_month = 'https://www.usbr.gov/mp/cvo/vungvari/doutdly.pdf'

    # The date where pdf gets small
    small_pdf = datetime.datetime.strptime('0117', '%m%y')
    prn_date = datetime.datetime.strptime('1210', '%m%y')
    special_date = datetime.datetime.strptime('0111',"%m%y")

    # Getting list of dates for url
    for month in months_between(start_date, end_date):
        dates = month.strftime("%m%y")
        new_month = datetime.datetime.strptime(dates, '%m%y')
        foo_dtime.append(new_month)
        foo.append(dates)

    # Using the list of dates, grab a url for each date
    for foos in foo:
        url = url_maker_doutdly(foos)
        urls.append(url)

    # Since the current month url is slightly different, we set up a condition that replaces that url with the correct one
    if today_month == int(e_month) & today_year == int(e_year):
        urls[-1] = current_month

    # Using the url, grab the pdf and concatenate it based off dates
    for j in range(len(urls)):
        print(j)

        if foo_dtime[j] > small_pdf:
            # means the pdf is in newer format
            pdf1 = read_pdf(urls[j], encoding = 'ISO-8859-1',stream=True, area = [175.19, 20.76,450.78 ,900.67], pages = 1, guess = False,  pandas_options={'header':None})
            pdf_df = df_generator(pdf1)

            # catches scenario that goes up to current date
            if urls[j] == current_month:
            # drop the last row, invalid date
                pdf_df = pdf_df.drop(pdf_df.tail(1).index,inplace=True)
                result = pd.concat([result,pdf_df])
            else:
                result = pd.concat([result,pdf_df])
            
        elif prn_date < foo_dtime[j] <= small_pdf:
            if foo_dtime[j] == special_date:
                pdf1 = read_pdf("https://www.usbr.gov/mp/cvo/vungvari/dout0111.pdf", encoding = 'ISO-8859-1',stream=True, area = [151.19, 20.76,350 ,733.67], pages = 1, guess = False,  pandas_options={'header':None})
                pdf_df = df_generator(pdf1)
                result = pd.concat([result,pdf_df])
            else:
                pdf1 = read_pdf(urls[j], encoding = 'ISO-8859-1',stream=True, area = [151.19, 20.76,360 ,900.67], pages = 1, guess = False,  pandas_options={'header':None})
                pdf_df = df_generator(pdf1)
                result = pd.concat([result,pdf_df])


        else:
            test = pd.read_fwf(urls[j], skiprows=10, skipfooter=2,
                   names=["Date", "SactoR_pd","SRTP_pd", "Yolo_pd","East_side_stream_pd","Joaquin_pd","Joaquin_7dy","Joaquin_mth", "total_delta_inflow",
    "NDCU", "CLT","TRA","CCC","BBID","NBA","total_delta_exports","3_dy_avg_TRA_CLT","NDOI_daily","outflow_7_dy_avg","outflow_mnth_avg","exf_inf_daily",
    "exf_inf_3dy","exf_inf_14dy"],
                  index_col=False,
                  colspecs = [ (0, 9)
                              ,(9, 16)
                              ,(16, 23)
                              ,(27,35)
                              ,(35, 43)
                              ,(43, 50)
                              ,(50,57)
                            ,(57,64)
                            ,(64,71)
                            ,(71,81)
                            ,(81,87)
                            ,(88,95)
                            ,(95,102)
                            ,(102,109)
                            ,(109,116)
                            ,(116,123)
                            ,(124,132)
                            ,(132,140)
                            ,(140,148)
                            ,(148,156)
                            ,(156,163)
                            ,(163,168)
                            ,(169,178)])
            result = pd.concat([result,test])


    # Extract date range 
    new_start_date = start_date.strftime("%m-%d-%y")
    new_end_date = end_date.strftime("%m-%d-%y")

    mask = (result['Date'] >= new_start_date) & (result['Date'] <= new_end_date)
    new_df = result.loc[mask]

    #Set DateTime Index
    new_df.set_index('Date', inplace = True)

    new_df = data_cleaner(new_df)

    # Setting up the multi columns
    bottom_level =["SactoR_pd","SRTP_pd", "Yolo_pd","East_side_stream_pd","Joaquin_pd","Joaquin_7dy","Joaquin_mth", "total_delta_inflow",
           "NDCU", 
           "CLT","TRA","CCC","BBID","NBA","total_delta_exports","3_dy_avg_TRA_CLT",
           "NDOI_daily","outflow_7_dy_avg","outflow_mnth_avg",
           "exf_inf_daily","exf_inf_3dy","exf_inf_14dy"]

    top_level = ['delta_inflow','delta_inflow','delta_inflow','delta_inflow','delta_inflow','delta_inflow','delta_inflow','delta_inflow',
         'NDCU',
         'delta_exports','delta_exports','delta_exports','delta_exports','delta_exports','delta_exports','delta_exports',
         'outflow_index','outflow_index','outflow_index',
         'exp_inf','exp_inf','exp_inf']
    arrays = [top_level,bottom_level]
    tuples = list(zip(*arrays))





    new_df.columns = pd.MultiIndex.from_tuples(tuples)
    # new_df.to_csv('dout_smallpdf_v3.csv')  

    return new_df 
    #return dates

