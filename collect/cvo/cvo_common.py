'''
collect.cvo.cvo_dout
==========================================================================
Functions that are used throughout the cvo scripts

Some will have multiple args to differentiate between the CVO data that is being read
'''

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
def url_maker(date,url):
    '''
    Arguments:
        date (str): the date but in MMYY format, ex 0520
        url (str): specify which pdf you're looking at
    Returns:
        url (str): the url for requesting the file
    '''
    if url == 'kesdop':
        text = "https://www.usbr.gov/mp/cvo/vungvari/kesdop"
        final = text + date + ".pdf"

        return str(final)

    elif url == 'shadop':
        text = "https://www.usbr.gov/mp/cvo/vungvari/shadop"
        final = text + date + ".pdf"

        return str(final)

    elif url == 'shafin':
        text = "https://www.usbr.gov/mp/cvo/vungvari/shafln"
        final = text + date + ".pdf"

        return str(final)


    elif url == 'dout':
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

def df_generator(ls,url):
    """
    Arguements: 
        dataframe in a [1,rows,columns] structure
        Change to an array and reshape it

        Minor adjustment for shadop files
    Returns:
        dataframe in a [rows,columns] structure, changes column names to the correct ones

    Function is specific to shadop
    """
    if url == 'kesdop':
        ls = np.array(ls)
        ls1 = ls[0]

        # Change from array to dataframe, generate new columns
        df = pd.DataFrame(ls1,columns=[ "Date","elev","storage","change","inflow",
        "spring_release", "shasta_release", "power","spill","fishtrap","evap_cfs"])


        return df

    elif url == 'shadop':
        ls = np.array(ls)
        ls1 = ls[0]

        # Change from array to dataframe, generate new columns
        df = pd.DataFrame(ls1,columns=[ "Date","elev","in_lake","change","inflow_cfs",
        "power", "spill", "outlet","evap_cfs","evap_in","precip_in"])


        return df

    elif url == 'shafin':
        ls = np.array(ls)
        ls1 = ls[0]

        # Change from array to dataframe, generate new columns
        df = pd.DataFrame(ls1,columns=["Date","britton","mccloud",
        "iron_canyon","pit6",
        "pit7", "res_total", "d_af",
        "d_cfs","shasta_inf","nat_river","accum_full_1000af"])

        return df


    elif url == 'dout':

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
    try:
        datetime.datetime.strptime(date_text, '%Y/%m/%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY/MM/DD")


# Making everything to a function
def data_cleaner(df,url):
    if url == 'dout':
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
                df.iloc[i][j] = df.iloc[i][j].replace('(','')
                df.iloc[i][j] = df.iloc[i][j].replace(')','')
        
        cols = df.columns
        n_rows = len(df)
        n_cols = len(cols)
        df[cols] = df[cols].astype(str)
        for i in range(n_rows):
            for j in range(n_cols):
                df.iloc[i][j] = df.iloc[i][j].replace(',','')
                df.iloc[i][j] = df.iloc[i][j].replace('%','')
                df.iloc[i][j] = df.iloc[i][j].replace('(','')
                df.iloc[i][j] = df.iloc[i][j].replace(')','')
    
        df[cols] = df[cols].astype(float)

        # Hardcoding it for the last 3 columns since we know that it is in percentages
        df.iloc[:,-3:] = df.iloc[:,-3:]/100
        return df
    else:
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