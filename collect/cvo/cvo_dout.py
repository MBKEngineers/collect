"""
collect.cvo.cvo_dout
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


def file_getter_dout(start, end):
    """
    Arguements:
        range of dates including start and end month
        Given in YYYY/MM/DD format

    Returns:
        dataframe of date range 
        date of the when the file was published
        datetime of all the months selected
        url of all the months selected

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
    date_published = []
    result = pd.DataFrame()
    current_month = 'https://www.usbr.gov/mp/cvo/vungvari/doutdly.pdf'

    # The date where pdf gets small
    small_pdf = datetime.datetime.strptime('0117', '%m%y')
    prn_date = datetime.datetime.strptime('1210', '%m%y')
    special_date = datetime.datetime.strptime('0111',"%m%y")
    blown_up_start = datetime.datetime.strptime('0120',"%m%y")
    blown_up_end = datetime.datetime.strptime('0820',"%m%y")

    # Getting list of dates for url
    for month in months_between(start_date, end_date):
        dates = month.strftime("%m%y")
        new_month = datetime.datetime.strptime(dates, '%m%y')
        foo_dtime.append(new_month)
        foo.append(dates)

    # Using the list of dates, grab a url for each date
    for foos in foo:
        url = url_maker(foos,'dout')
        urls.append(url)

    e_month = int(e_month)
    e_year = int(e_year)
    # Since the current month url is slightly different, we set up a condition that replaces that url with the correct one
    if today_month == e_month and today_year == e_year:
        test = 1
        urls[-1] = current_month
    else:
        pass
    
    ### Get the dates when data was published
    for j in range(len(urls)):

        if foo_dtime[j] > small_pdf:
            # catches scenario that goes up to current date
            if  (blown_up_start <= foo_dtime[j] <= blown_up_end) or (datetime.datetime.strptime('0319',"%m%y") <= foo_dtime[j] <= datetime.datetime.strptime('0819',"%m%y")):
                pdf1 = read_pdf(urls[j], encoding = 'ISO-8859-1',stream=True,area = [900, 850,1200.78 ,1400.67], pages = 1,  pandas_options={'header':None})
                pdf_time= pd.to_datetime(pdf1[0][0])
                date_published.append(pdf_time)
            else:
                pdf1 = read_pdf(urls[j], encoding = 'ISO-8859-1',stream=True,area = [566, 566,700 ,800], pages = 1,  pandas_options={'header':None})
                pdf_time= pd.to_datetime(pdf1[0][0])
                date_published.append(pdf_time)
        
        else:
          pdf1 = read_pdf(urls[j], encoding = 'ISO-8859-1',stream=True,area = [566, 566,700 ,800], pages = 1,  pandas_options={'header':None})
          pdf_time= pd.to_datetime(pdf1[0][0])
          date_published.append(pdf_time)

    # Using the url, grab the pdf and concatenate it based off dates
    for j in range(len(urls)):

        if foo_dtime[j] > small_pdf:
            # catches scenario that goes up to current date
            if  (blown_up_start <= foo_dtime[j] <= blown_up_end) or (datetime.datetime.strptime('0319',"%m%y") <= foo_dtime[j] <= datetime.datetime.strptime('0819',"%m%y")):
                pdf1 = read_pdf(urls[j], encoding = 'ISO-8859-1',stream=True, area = [290.19, 20.76,750.78 ,1300.67], pages = 1, guess = False,  pandas_options={'header':None})
                pdf_df = df_generator(pdf1,'dout')
                result = pd.concat([result,pdf_df])
            else:
                pdf1 = read_pdf(urls[j], encoding = 'ISO-8859-1',stream=True, area = [175.19, 20.76,450.78 ,900.67], pages = 1, guess = False,  pandas_options={'header':None})
                pdf_df = df_generator(pdf1,'dout')
                result = pd.concat([result,pdf_df])
        
        elif prn_date < foo_dtime[j] <= small_pdf:
            # Weird date where pdf gets slightly longer
            if foo_dtime[j] == special_date:
                pdf1 = read_pdf("https://www.usbr.gov/mp/cvo/vungvari/dout0111.pdf", encoding = 'ISO-8859-1',stream=True, area = [151.19, 20.76,350 ,733.67], pages = 1, guess = False,  pandas_options={'header':None})
                pdf_df = df_generator(pdf1,'dout')
                result = pd.concat([result,pdf_df])
            else:
                pdf1 = read_pdf(urls[j], encoding = 'ISO-8859-1',stream=True, area = [151.19, 20.76,360 ,900.67], pages = 1, guess = False,  pandas_options={'header':None})
                pdf_df = df_generator(pdf1,'dout')
                result = pd.concat([result,pdf_df])


        else:
            test = pd.read_fwf(urls[j], skiprows=10, skipfooter=2,
                   names=["Date", "SactoR_pd","SRTP_pd", "Yolo_pd","East_side_stream_pd","Joaquin_pd","Joaquin_7dy","Joaquin_mth", "total_delta_inflow",
    "NDCU", "CLT","TRA","CCC","BBID","NBA","total_delta_exports","3_dy_avg_TRA_CLT","NDOI_daily","outflow_7_dy_avg","outflow_mnth_avg","exf_inf_daily",
    "exf_inf_3dy","exf_inf_14dy"],
                  index_col=False,
                  colspecs = [ (0, 9)
                              ,(9, 16)
                              ,(16, 25)
                              ,(25,34)
                              ,(34, 42)
                              ,(42, 50)
                              ,(50,57)
                            ,(57,64)
                            ,(64,71)
                            ,(71,81)
                            ,(81,88)
                            ,(88,95)
                            ,(95,102)
                            ,(102,109)
                            ,(109,115)
                            ,(115,124)
                            ,(124,132)
                            ,(132,140)
                            ,(140,147)
                            ,(147,156)
                            ,(156,163)
                            ,(163,169)
                            ,(169,177)])
            result = pd.concat([result,test])

    result['Date'] = pd.to_datetime(result['Date'])

    # Extract date range 
    new_start_date = start_date.strftime("%m-%d-%y")
    new_end_date = end_date.strftime("%m-%d-%y")
    mask = (result['Date'] >= new_start_date) & (result['Date'] <= new_end_date)
    new_df = result.loc[mask]

    #Set DateTime Index
    new_df.set_index('Date', inplace = True)

    new_df = data_cleaner(new_df,'dout')

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
    
    return {'data': new_df, 'info': {'url': urls,
                                 'title': " U.S. Bureau of Reclamation - Central Valley Operations Office Delta Outflow Computation",
                                 'units': 'cfs',
                                 'date published': date_published}}
    #return dates


test_cases = ['2005/01/01','2004/01/01',
'2003/01/01','2002/01/01','2001/01/01']
test_end = ['2008/01/01','2007/01/01','2006/01/01',
'2005/01/01','2004/01/01']

data = file_getter_dout('2015/04/15','2022/04/20')
'''
,'2012/01/01','2011/01/01','2010/01/01','2009/01/01',
'2008/01/01','2007/01/01','2006/01/01','2005/01/01','2004/01/01',
'2003/01/01','2002/01/01','2001/01/01'
'''
# testing = ['2022/01/01','2021/01/01']

# for i in range(len(test_cases)):
#     start_test_date = test_cases[i]
#     start_test_end = test_end[i]
#     data = file_getter_dout(start_test_date,start_test_end)
#     print(f"It works for {start_test_date} to {start_test_end}")



# print(data)
#json derulo
'''
file_getter_dout
data.index = data.index.strftime('%Y-%m-%d %H:%M')

data['NDCU']['NDCU'].to_json(path_or_buf = "ndcu_v1.json", orient = 'index', date_format = 'iso')
'''