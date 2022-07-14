"""
collect.cvo.cvo_dout
============================================================
access cvo data
"""
# -*- coding: utf-8 -*-

import datetime
from datetime import date
from email.utils import collapse_rfc2231_value 

import pandas as pd
from tabula import read_pdf

from collect.cvo.cvo_common import url_maker, months_between, df_generator, validate, data_cleaner


def file_getter_dout(start, end):
    """
    Arguements:
        range of dates including start and end month
        Given in datetime format

    Returns:
        dataframe of date range 
        date of the when the file was published
        datetime of all the months selected
        url of all the months selected

    """
    # Check if date is in the right format
    validate(start)
    validate(end)

    today_date = datetime.datetime.now()
    today_month = int(today_date.strftime('%m'))
    today_year = int(today_date.strftime('%Y'))

    # Defining variables
    date_list = []
    date_list_dtime = []
    urls = []
    date_published = []
    result = pd.DataFrame()
    current_month = 'https://www.usbr.gov/mp/cvo/vungvari/doutdly.pdf'

    # Dates of specific changes to pdf sizing
    small_pdf = datetime.datetime.strptime('0117', '%m%y')
    prn_date = datetime.datetime.strptime('1210', '%m%y')
    special_date = datetime.datetime.strptime('0111',"%m%y")
    blown_up_start1 = datetime.datetime.strptime('0120',"%m%y")
    blown_up_end1 = datetime.datetime.strptime('0820',"%m%y")

    blown_up_start2 = datetime.datetime.strptime('0319',"%m%y")
    blown_up_end2 = datetime.datetime.strptime('0819',"%m%y")

    blown_up_start3 = datetime.datetime.strptime('0622',"%m%y")

    # Getting list of dates for url
    for month in months_between(start, end):
        # converting to dates like 0511, 0718 etc, mmyy
        dates = month.strftime("%m%y")
        # converting to dates like 0511, 0718 to a datetime format
        new_month = datetime.datetime.strptime(dates, '%m%y')
        date_list_dtime.append(new_month)
        date_list.append(dates)

        url = url_maker(dates,'dout')
        urls.append(url)

    # Since the current month url is slightly different, 
    # we set up a condition that replaces that url with the correct one
    if today_month == int(end.strftime('%m')) and today_year == int(end.strftime('%Y')):
        urls[-1] = current_month
    else:
        pass

    # Using the url, read the pdf and concatenate it based off dates
    # In addition, read the date published and store it
    for j in range(len(urls)):

        if date_list_dtime[j] > small_pdf:
            # catches scenario that goes up to current date
            # Some months have PDFs which are larger in size
            range1 = blown_up_start1 <= date_list_dtime[j] <= blown_up_end1
            range2 = (blown_up_start2 <= date_list_dtime[j] <= blown_up_end2)
            range3 =(blown_up_start3 <= date_list_dtime[j] <= today_date)
            if range1 or range2 or range3:
                pdf1 = read_pdf(urls[j], encoding = 'ISO-8859-1',stream=True, area = [290.19, 20.76,750.78 ,1300.67], pages = 1, guess = False,  pandas_options={'header':None})
                pdf2 = read_pdf(urls[j], encoding = 'ISO-8859-1',stream=True, area = [900, 850,1200.78 ,1400.67], pages = 1,  pandas_options={'header':None})

            else:
                pdf1 = read_pdf(urls[j], encoding = 'ISO-8859-1',stream=True, area = [175.19, 20.76,450.78 ,900.67], pages = 1, guess = False,  pandas_options={'header':None})
                pdf2 = read_pdf(urls[j], encoding = 'ISO-8859-1',stream=True, area = [566, 566,700 ,800], pages = 1,  pandas_options={'header':None})

            pdf_df = df_generator(pdf1,'dout')
            result = pd.concat([result,pdf_df])
        
        elif prn_date < date_list_dtime[j] <= small_pdf:
            # Weird date where pdf gets slightly longer
            # Other PDFs are smaller than the usual size
            if date_list_dtime[j] == special_date:
                pdf1 = read_pdf("https://www.usbr.gov/mp/cvo/vungvari/dout0111.pdf", encoding = 'ISO-8859-1',stream=True, area = [151.19, 20.76,350 ,733.67], pages = 1, guess = False,  pandas_options={'header':None})
            else:
                pdf1 = read_pdf(urls[j], encoding = 'ISO-8859-1',stream=True, area = [151.19, 20.76,360 ,900.67], pages = 1, guess = False,  pandas_options={'header':None})
            pdf2 = read_pdf(urls[j], encoding = 'ISO-8859-1',stream=True, area = [566, 566,700 ,800], pages = 1,  pandas_options={'header':None})

            pdf_df = df_generator(pdf1,'dout')
            result = pd.concat([result,pdf_df])


        else:
            # prn file format
            time = pd.read_fwf(urls[j],index_col = False,infer_nrows = 0)
            pdf2 = [time.columns]

            test = pd.read_fwf(urls[j], skiprows=10, skipfooter=2,
                   names=["Date", "SactoR_pd","SRTP_pd", "Yolo_pd","East_side_stream_pd","Joaquin_pd",
                   "Joaquin_7dy","Joaquin_mth", "total_delta_inflow","NDCU", "CLT","TRA","CCC","BBID",
                   "NBA","total_delta_exports","3_dy_avg_TRA_CLT","NDOI_daily","outflow_7_dy_avg",
                   "outflow_mnth_avg","exf_inf_daily","exf_inf_3dy","exf_inf_14dy"],
                  index_col=False,
                  colspecs =[(0, 9),(9, 16),(16, 25),(25,34),(34, 42),
                  (42, 50),(50,57),(57,64),(64,71),(71,81),(81,88),(88,95),
                  (95,102),(102,109),(109,115),(115,124),(124,132),(132,140),
                  (140,147),(147,156),(156,163),(163,169),(169,177)])
            result = pd.concat([result,test])

        pdf_time= pd.to_datetime(pdf2[0][0])
        date_published.append(pdf_time)

    result['Date'] = pd.to_datetime(result['Date'])
    

    # Extract date range 
    new_start_date = start.strftime("%m-%d-%y")
    new_end_date = end.strftime("%m-%d-%y")
    mask = (result['Date'] >= new_start_date) & (result['Date'] <= new_end_date)
    new_df = result.loc[mask]

    #Set DateTime Index
    new_df.set_index('Date', inplace = True)

    new_df = data_cleaner(new_df,'dout')

    # tuple format: (top, bottom)
    tuples = (('delta_inflow','SactoR_pd'),('delta_inflow','SRTP_pd'),('delta_inflow','Yolo_pd'),
    ('delta_inflow','East_side_stream_pd'),('delta_inflow','Joaquin_pd'),('delta_inflow','Joaquin_7dy'),
    ('delta_inflow','Joaquin_mth'),('delta_inflow','total_delta_inflow'),('NDCU','NDCU'),
    ('delta_exports','CLT'),('delta_exports','TRA'),('delta_exports','CCC'),('delta_exports','BBID'),
    ('delta_exports','NBA'),('delta_exports','total_delta_exports'),('delta_exports','3_dy_avg_TRA_CLT'),
    ('outflow_index','NDOI_daily'),('outflow_index','outflow_7_dy_avg'),('outflow_index','outflow_mnth_avg'),
    ('exp_inf','exf_inf_daily'),('exp_inf','exf_inf_3dy'),('exp_inf','exf_inf_14dy'))
    new_df.columns = pd.MultiIndex.from_tuples(tuples)
    # new_df.to_csv('dout_smallpdf_v3.csv')  
    print(date_published)

    return {'data': new_df, 'info': {'url': urls,
                                 'title': " U.S. Bureau of Reclamation - Central Valley Operations Office Delta Outflow Computation",
                                 'units': 'cfs',
                                 'date published': date_published}}
    #return dates
# if __name__ == '__main__':

#     start_date = datetime.datetime(2010,5,10)
#     end_date = datetime.datetime(now)

#     data = file_getter_dout(start_date,end_date)

