# Nevada ID collect
import io
import requests
import pandas as pd
import numpy as np
import json
import re

from bs4 import BeautifulSoup
from calendar import month_abbr

sites = {
    'BR100': 'Auburn Ravine I at Head',
    'BR220': 'Hemphill Canal at Head',
    'BR301': 'Combie Phase I at Head',
    'BR334': 'Camp Far West at Head',
    'BR368': 'Gold Hill I at Head',
    'BR900': 'Combie Reservoir-Spill-1600.',
    'BSCA': 'Bowman-Spaulding Canal Intake Near Graniteville, Ca',
    'BWMN': 'Bowman Lake Near Graniteville, Ca',
    'CPFL': 'Chicago Park Flume Near Dutch Flat, Ca',
    'DC102': 'Cascade at Head',
    'DC131': 'Newtown Canal at Head',
    'DC140': 'Tunnel Canal at Head',
    'DC145': 'D. S. Canal at Head',
    'DC169': 'Tarr Canal at Head',
    'DC900': 'Scott''s Flat Reservoir',
    'DFFL': 'Dutch Flat #2 Flume Near Blue Canyon, Ca',
    'FAUC': 'Faucherie Lake Near Cisco, Ca',
    'FRLK': 'French Lake Near Cisco Grove, Ca',
    'JKSN': 'Jackson Lake near Sierra City',
    'JMDW': 'Jackson Meadows Reservoir Near Sierra City, Ca',
    'MBTO': 'Milton-Bowman Tunnel Outlet (South Portal)',
    'ROLK': 'Rollins Reservoir Near Colfax, Ca',
    'SWML': 'Sawmill Lake Near Graniteville, Ca',
    'WLSN': 'Wilson Creek near Sierra City'
}

months = ['OCT', 'NOV', 'DEC', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP']
current_year = 2021

json_data = {}
json_data['info'] = []

for site, name in sites.items():

    site_index = f'https://river-lake.nidwater.com/hyquick/{site}/index.htm'

    soup = BeautifulSoup(requests.get(site_index).content, 'lxml')
    rows = soup.find_all('td')

    for row in rows:
        try:
            metric = re.search(r'usday_daily_(.*?)\.txt', str(row.a)).group(1)
        except:
            continue

    data_url = f'https://river-lake.nidwater.com/hyquick/{site}/{site}.usday_daily_{metric}.txt'

    if metric == 'flow':
        skiprows = [0,1,2,3,4,5,6,7,8,9,12] + list(range(49,500))
    elif metric == 'volume':
        skiprows = [0,1,2,3,4,5,6,7,8,9,10,12,14] + list(range(52,500))

    data = pd.read_fwf(data_url, skiprows=skiprows, header=1)
    data.dropna(how='all', inplace=True)

    data['Day'] = data['Day'].astype(int)
    data.replace('------', np.NaN, inplace=True)

    df = pd.DataFrame()
    for month in months:
        month_df = pd.DataFrame()

        if month in ['OCT', 'NOV', 'DEC']:
            year = current_year -1
        else:
            year = current_year

        month_df['date'] = data['Day'].apply(lambda x: str(x) + month + str(current_year))
        month_df[metric] = data[month]

        df = pd.concat((df, month_df))

    df.dropna(inplace=True)
    df[metric] = pd.to_numeric(df[metric])
    df.index = df['date']

    new_index = pd.date_range(start=df.index[0], end=df.index[-1])
    df = df.reindex(new_index)
    df.index = df.index.strftime('%Y-%-m-%-d 00:00')

    df.fillna('null', inplace=True)

    timeseriesType = {'flow': 'flows', 'volume': 'storages'}.get(metric)
    timeseriesUnits = {'flow': 'cfs', 'volume': 'AF'}.get(metric)

    json_data['info'].append({
        'site': site,
        'url': data_url,
        'timeseriesType': timeseriesType,
        'timeseriesUnits': timeseriesUnits,
        'timeseries': df[metric].to_dict()
    })

with open('NID_daily.json', 'w') as outfile:
    json.dump(json_data['info'], outfile)

