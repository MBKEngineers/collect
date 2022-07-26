"""
collect.cvo.common
==========================================================================
Functions that are used throughout the cvo scripts

Some will have multiple args to differentiate between the CVO data that is being read
"""

# -*- coding: utf-8 -*-

import datetime
from datetime import date

import pandas as pd
import numpy as np

# Remove false positive warning about editing dataframes
pd.options.mode.chained_assignment = None  # default='warn'


def report_type_maker(date, report_type):
    '''
    Arguments:
        date (str): the date but in MMYY format, ex 0520
        report_type (str): specify which pdf you're looking at
    Returns:
        report_type (str): the report_type for requesting the file
    '''
    
    if report_type == 'kesdop':
        text = "https://www.usbr.gov/mp/cvo/vungvari/kesdop"
        final = text + date + ".pdf"

        return str(final)

    elif report_type == 'shadop':
        text = "https://www.usbr.gov/mp/cvo/vungvari/shadop"
        final = text + date + ".pdf"

        return str(final)

    elif report_type == 'shafln':
        text = "https://www.usbr.gov/mp/cvo/vungvari/shafln"
        final = text + date + ".pdf"

        return str(final)

    elif report_type == 'dout':

        # construct the station report_type
        prn = datetime.date(2010, 12, 1)
        txt = datetime.date(2002, 4, 1)
        file_date = datetime.date(2000+int(date[2:4]), int(date[0:2]),1)
        
        # if date is before including 2010 December, give the prn link
        if txt < file_date <= prn:
            text = "https://www.usbr.gov/mp/cvo/vungvari/dout"
            final = text + date + ".prn"

        # if date is before including 2002 April, give the txt link
        elif file_date <= txt:
            text = "https://www.usbr.gov/mp/cvo/vungvari/dout"
            final = text + date + ".txt"

        # if not give in pdf format
        else:
            text = "https://www.usbr.gov/mp/cvo/vungvari/dout"
            final = text + date + ".pdf"

        return str(final)


def months_between(start_date, end_date):
    """
    Given two instances of ``datetime.datetime``, generate a list of dates on
    the 1st of every month between the two dates (inclusive).

    Arguments:
        start_date (datetime.datetime):  start date given by user input
        end_date (datetime.datetime): end date given by user input
    Yields:
       (datetime.date): all dates between the date range in mmyy format
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
            

def load_pdf_to_dataframe(ls,report_type):
    """
    Changes dataframe to an array and reshape it
    column names change to what is specified below

    Arguments:
        ls (dataframe):  dataframe in a [1,rows,columns] structure
        report_type (string): name of report

    Returns:
        df (dataframe): in a [rows,columns] structure, 

    """

    # establishing column names
    kesdop_col_names =["Date", "elev", "storage", "change", "inflow",
                        "spring_release", "shasta_release", "power", 
                        "spill", "fishtrap", "evap_cfs"]

    shadop_col_names =["Date", "elev", "in_lake", "change", "inflow_cfs", 
                        "power", "spill", "outlet", "evap_cfs", "evap_in", 
                        "precip_in"]

    shafln_col_names =["Date", "britton", "mccloud", "iron_canyon", "pit6",
                        "pit7", "res_total", "d_af", "d_cfs", "shasta_inf", 
                        "nat_river", "accum_full_1000af"]

    dout_col_names =["Date", "SactoR_pd", "SRTP_pd", "Yolo_pd", "East_side_stream_pd", 
                    "Joaquin_pd", "Joaquin_7dy","Joaquin_mth", "total_delta_inflow", 
                    "NDCU", "CLT", "TRA", "CCC", "BBID", "NBA", "total_delta_exports", 
                    "3_dy_avg_TRA_CLT", "NDOI_daily","outflow_7_dy_avg", 
                    "outflow_mnth_avg", "exf_inf_daily","exf_inf_3dy", "exf_inf_14dy"]



    # changing structure of dataframe
    ls = np.array(ls)
    ls1 = ls[0]

    if report_type == 'kesdop':
        # Change from array to dataframe, generate new columns
        df = pd.DataFrame(ls1,columns=kesdop_col_names)

    elif report_type == 'shadop':
        df = pd.DataFrame(ls1,columns=shadop_col_names)

    elif report_type == 'shafln':
        df = pd.DataFrame(ls1,columns=shafln_col_names)

    elif report_type == 'dout':
        df = pd.DataFrame(ls1,columns=dout_col_names)

    return df.dropna().reindex()


def validate_user_date(date_text):
    """
    Checks if users date is in valid format
    Arguments:
        date_text (user input): date of user input
    Returns:
        None: if user input is not datetime the process will end
    """

    #if condition returns True, then nothing happens:
    assert isinstance(date_text,datetime.date) == True , "Please give in date format"


def data_cleaner(df,report_type):
    """
    This function converts data from string to floats and
    removes any non-numeric elements 

    Two seperate conditions for dout and kesdop,shadop,shafln

    Arguements:
        dataframe that was retreieved from file_getter function

    Returns:
        dataframe of strings converted to floats for data analysis
    """
    if report_type == 'dout':
        for key, value in df.iteritems():
            value = value.astype(str)
            value = value.replace(to_replace = r'[,\/]',value = '', regex =True)
            value = value.replace(to_replace = r'[%\/]',value = '', regex =True)
            value = value.replace(to_replace = r'[(\/]',value = '', regex =True)
            value = value.replace(to_replace = r'[)\/]',value = '', regex =True)

            df.loc[:,key] = value.astype(float)

        return df

    else:
        for key, value in df.iteritems():
            value = value.astype(str)
            value = value.replace(to_replace = r'[,\/]',value = '', regex =True)
            value = value.replace(to_replace = r'[%\/]',value = '', regex =True)

            df.loc[:,key] = value.astype(float)
    
        return df