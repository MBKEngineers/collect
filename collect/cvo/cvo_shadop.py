"""
collect.cvo.cvo_shadop
============================================================
access cvo data
"""
# -*- coding: utf-8 -*-
import datetime
from datetime import date 

import pandas as pd
from tabula import read_pdf

from collect.cvo.cvo_common import url_maker, months_between, df_generator, validate, data_cleaner


# input as a range of dates
# Takes range of dates and uses url_maker to get multiple pdfs
# Format: 'YYYY/MM/DD'
def file_getter_shadop(start, end):
    """
    Earliest PDF date: Feburary 2000

    Arguements:
        Range of dates including start and end month
        Given in YYYY/MM/DD format and as a datetime object

    Returns:
        dataframe of date range 

    """
    # Check if date is in the right format
    validate(start)
    validate(end)

    today_date = date.today()
    today_month = today_date.month

    # Defining variables
    date_list = []
    urls = []
    date_published = []
    result = pd.DataFrame()
    current_month = 'https://www.usbr.gov/mp/cvo/vungvari/shadop.pdf'

	# Getting list of dates for url
    for month in months_between(start, end):
        date_published.append(month)
        dates = month.strftime("%m%y")
        date_list.append(dates)


	# Using the list of dates, grab a url for each date
    for date_url in date_list:
        url = url_maker(date_url,'shadop')
        urls.append(url)

	# Since the current month url is slightly different, 
    # we set up a condition that replaces that url with the correct one
    if today_month == end.month:
        urls[-1] = current_month

	# Using the url, grab the pdf and concatenate it based off dates
    count = 0
    for links in urls:
		# Finding out if it is in feburary or not
        month = links[-8:-6]
        if month == '02':
            Area = [140,30,435,540]
            
        elif links == current_month:
            today_day = today_date.day
            # today date-1 since the file does not include current date
            bottom = 140 + (today_day)*10
            Area = [145, 45,bottom,540]

        else:
            Area = [140,30,460,540]
            
        pdf1 = read_pdf(links,
		        stream=True, area = Area, pages = 1, guess = False,  pandas_options={'header':None})
        pdf_df = df_generator(pdf1,'shadop')

		# change the dates in pdf_df to datetime
        default_time = '00:00:00'
        correct_dates = []
        for i in range(len(pdf_df['Date'])):
            day = str(pdf_df['Date'][i])
            day = day.zfill(2)

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

    new_df = data_cleaner(new_df,'shadop')
    # tuple format: (top, bottom)

    tuples = (('Elevation','elev'),
    ('Storage_1000AF','in_lake'),('Storage_1000AF','change'),
    ('CFS','inflow_cfs'),
    ('Release_CFS','power'),('Release_CFS','spill'),('Release_CFS','outlet'),
    ('Evaporation','evap_cfs'),('Evaporation','evap_in'),
    ('Precip_in','precip_in'))
    new_df.columns = pd.MultiIndex.from_tuples(tuples)

    print(new_df.head())

    return {'data': new_df, 'info': {'url': urls,
                                 'title': "Shadop Reservoir Daily Operations",
                                 'units': 'cfs',
                                 'date published': date_published,
                                 'date retrieved': today_date}}

start_date = datetime.date(2022,1,10)
end_date = datetime.date(2022,7,10)

data = file_getter_shadop(start_date,end_date)
print(data)

if __name__ == '__main__':
    start_date = datetime.date(2021,1,10)
    end_date = datetime.today()
    data = file_getter_shadop(start_date,end_date)
