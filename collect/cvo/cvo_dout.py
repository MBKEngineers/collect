"""
collect.cvo.cvo_dout
============================================================
access cvo data
"""
# -*- coding: utf-8 -*-

from datetime import datetime
import dateutil.parser
from numpy import full

import pandas as pd
try:
    from tabula import read_pdf
except:
    print('WARNING: tabula is require for cvo module')
import re

from collect.cvo.cvo_common import report_type_maker, months_between, load_pdf_to_dataframe, validate_user_date, data_cleaner

def file_getter_dout(start, end, report_type = 'dout'):
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
    validate_user_date(start)
    validate_user_date(end)

    today_date = datetime.now()

    # Defining variables
    frames = []
    urls = []
    dates_published = []
    result = pd.DataFrame()



    # Dates of specific changes to pdf sizing
    small_pdf = datetime.strptime('0117', '%m%y')
    prn_date = datetime.strptime('1210', '%m%y')
    txt_date = datetime.strptime('0402', '%m%y')
    special_date = datetime.strptime('0111','%m%y')
    blown_up_start1 = datetime.strptime('0120','%m%y')
    blown_up_end1 = datetime.strptime('0820','%m%y')

    blown_up_start2 = datetime.strptime('0319','%m%y')
    blown_up_end2 = datetime.strptime('0819','%m%y')

    blown_up_start3 = datetime.strptime('0622','%m%y')

    column_names=['Date', 'SactoR_pd','SRTP_pd', 'Yolo_pd','East_side_stream_pd','Joaquin_pd',
                   'Joaquin_7dy','Joaquin_mth', 'total_delta_inflow','NDCU', 'CLT','TRA','CCC','BBID',
                   'NBA','total_delta_exports','3_dy_avg_TRA_CLT','NDOI_daily','outflow_7_dy_avg',
                   'outflow_mnth_avg','exf_inf_daily','exf_inf_3dy','exf_inf_14dy']

    # Getting list of dates for url
    for dt_month in months_between(start, end):
        dt_month = datetime.strptime(dt_month.strftime('%m%y'), '%m%y')

        url = report_type_maker(dt_month.strftime('%m%y'),report_type)
        urls.append(url)

        if dt_month.strftime('%Y-%m') == today_date.strftime('%Y-%m'):
            url = 'https://www.usbr.gov/mp/cvo/vungvari/doutdly.pdf'
            full_data = read_pdf(url, encoding = 'ISO-8859-1',stream=True, area = [290.19, 20.76,750.78 ,1300.67], pages = 1, guess = False,  pandas_options={'header':None})
            date_pulished = read_pdf(url, encoding = 'ISO-8859-1',stream=True, area = [900, 850,1200.78 ,1400.67], pages = 1,  pandas_options={'header':None})
            urls[-1] = url

        elif (blown_up_start1 <= dt_month <= blown_up_end1) or (blown_up_start2 <= dt_month <= blown_up_end2) or (blown_up_start3 <= dt_month <= today_date):
            full_data = read_pdf(url, encoding = 'ISO-8859-1',stream=True, area = [290.19, 20.76,750.78 ,1300.67], pages = 1, guess = False,  pandas_options={'header':None})
            date_pulished = read_pdf(url, encoding = 'ISO-8859-1',stream=True, area = [900, 850,1200.78 ,1400.67], pages = 1,  pandas_options={'header':None})
                
        
        
        elif prn_date < dt_month <= small_pdf:
            # Weird date where pdf gets slightly longer
            # Other PDFs are smaller than the usual size
            if dt_month == special_date:
                full_data = read_pdf('https://www.usbr.gov/mp/cvo/vungvari/dout0111.pdf', encoding = 'ISO-8859-1',stream=True, area = [146.19, 20.76,350 ,733.67], pages = 1, guess = False,  pandas_options={'header':None})
            else:
                full_data = read_pdf(url, encoding = 'ISO-8859-1',stream=True, area = [151.19, 20.76,360 ,900.67], pages = 1, guess = False,  pandas_options={'header':None})
            date_pulished = read_pdf(url, encoding = 'ISO-8859-1',stream=True, area = [566, 566,700 ,800], pages = 1,  pandas_options={'header':None})

        elif txt_date < dt_month <= prn_date:
            try: 
            # prn file format
                time = pd.read_fwf(url,index_col = False,infer_nrows = 0)
                date_pulished = [time.columns]

                full_data = pd.read_table(url, skiprows=10, skipfooter=2,
                    names=column_names,
                    index_col=False,
                    delim_whitespace=True)
                
                for key, row in full_data.iterrows():
                    if len(row['outflow_7_dy_avg']) > 10: 
                        day = re.split('(\d{3},\d{3})', row['outflow_7_dy_avg'],maxsplit=1)[1:]
                        full_data.loc[key,'outflow_7_dy_avg':] = row.shift(1) 
                        full_data.loc[key,'outflow_7_dy_avg':'outflow_mnth_avg'] = day[0],day[1]
  
            except pd.errors.EmptyDataError:
                pass

                    
        elif dt_month <= txt_date: 
            # txt file format
            time = pd.read_fwf(url,index_col = False,infer_nrows = 0)
            date_pulished = [time.columns]

            txt = pd.read_csv(url, skiprows=10 , sep='\s{1,}',index_col = False, names=column_names)  
            full_data = txt[:-2]  

        else:
            full_data = read_pdf(url, encoding = 'ISO-8859-1',stream=True, area = [175.19, 20.76,450.78 ,900.67], pages = 1, guess = False,  pandas_options={'header':None})
            date_pulished = read_pdf(url, encoding = 'ISO-8859-1',stream=True, area = [566, 566,700 ,800], pages = 1,  pandas_options={'header':None})
            full_data = full_data[0]

        pdf_time= pd.to_datetime(date_pulished[0][0])
        dates_published.append(pdf_time)

        pdf_df = load_pdf_to_dataframe(full_data,report_type)
        pdf_df['Date'] = pdf_df['Date'].apply(lambda x: dateutil.parser.parse(x))
        frames.append(pdf_df)

    # result['Date'] = pd.to_datetime(result['Date'])

    df = pd.concat(frames).set_index('Date').sort_index().truncate(before=start, after=end)

    new_df = data_cleaner(df,report_type)


    # tuple format: (top, bottom)

    # new_df.to_csv('dout_full.csv')  
    # print(date_published)

    return {'data': new_df, 'info': {'url': urls,
                                 'title': 'U.S. Bureau of Reclamation - Central Valley Operations Office Delta Outflow Computation',
                                 'units': 'cfs',
                                 'date published': dates_published}}
    #return dates

if __name__ == '__main__':

    start_date = datetime(2022,6,1)
    end_date = datetime.now()

    data = file_getter_dout(start_date,end_date)

    print(data['data'])



