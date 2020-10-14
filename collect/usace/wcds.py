"""
collect.usace.wcds
============================================================
USACE Water Control Data System (WCDS)
"""
# -*- coding: utf-8 -*-
import datetime as dt
from datetime import datetime
import io
import pandas as pd
import requests
from collect.utils import get_water_year


def get_water_year_data(reservoir, water_year, interval='d'):
    """
    Scrape water year operations data from Folsom entry on USACE-SPK's WCDS.
    Note: times formatted as 2400 are assigned to 0000 of the next date. (hourly and daily)

    Arguments:
        reservoir (str): three-letter reservoir code; i.e. 'fol'
        water_year (int): the water year
        interval (str): data interval; i.e. 'd' 

    Returns:
        result (dict): query result dictionary with 'data' and 'info' keys
    """

    # USACE-SPK Folsom page
    url = f'https://www.spk-wc.usace.army.mil/plots/csv/{reservoir}{interval}_{water_year}.plot'

    # Read url data
    response=requests.get(url, verify=False).content
    df = pd.read_csv(io.StringIO(response.decode('utf-8')), header=0, na_values='-')

    # Clean zeros in note columns
    note_columns = ['Top of Conservation notes','Storage notes', 'Elevation notes', 'Inflow notes', 'Outflow notes', 'Precip at Dam notes'] 
    df[note_columns] = df[note_columns].replace(0, float('NaN'))

    # Convert to date time object
    df.set_index('ISO 8601 Date Time', inplace=True)
    df.index = pd.to_datetime(df.index)

    return {'data': df, 'info': {'reservoir': reservoir,'water year': water_year, 'interval':interval}}


    
def get_wcds_data(reservoir, start_time, end_time, interval='d'): #trim this to start and end
    """
    Scrape water year operations data from reservoir page on USACE-SPK's WCDS.
    
    Arguments:
        reservoir (str): three-letter reservoir code
        start_time (datetime.datetime): query start datetime
        end_time (datetime.datetime): query end datetime
        interval (str): data interval
    Returns:
        result (dict): query result dictionary with data and info keys
    """
    earliest_time = datetime.strptime('1994-10-01', '%Y-%m-%d')

    if start_time < earliest_time:
        print('Select later start date')
        return

    frames = []
    for water_year in range(get_water_year(start_time), get_water_year(end_time) + 1):
        frames.append(get_water_year_data(reservoir, water_year, interval)['data'])


    df = pd.concat(frames)
    df.index.name = 'ISO 8601 Date Time'
    df.to_csv('water_year.csv')

    return {'data': df, 'info': {'reservoir': reservoir, 
                                 'interval': interval, 
                                 'notes': 'daily data value occurs on midnight of entry date'}}


def get_wcds_reservoirs():
    """
    Corps and Section 7 Projects in California
    http://www.spk-wc.usace.army.mil/plots/california.html
    """
    csv_data = StringIO("""Region|River Basin|Agency|Project|WCDS_ID|Hourly Data|Daily Data
                            Sacramento Valley|Sacramento River|USBR|Shasta Dam & Lake Shasta|SHA|False|True
                            Sacramento Valley|Stony Creek|COE|Black Butte Dam & Lake|BLB|True|True
                            Sacramento Valley|Feather River|DWR|Oroville Dam & LakeOroville|ORO|False|True
                            Sacramento Valley|Yuba River|YCWA|New Bullards Bar Dam & Lake|BUL|False|True
                            Sacramento Valley|Yuba River|COE|Englebright Lake|ENG|True|True
                            Sacramento Valley|N. F. Cache Creek|YCFCWCA|Indian Valley Dam & Reservoir|INV|False|True
                            Sacramento Valley|American River|USBR|Folsom Dam & Lake|FOL|True|True
                            Sacramento Valley|American River|USBR|Folsom Dam & Lake|FOLQ|True|False
                            San Joaquin Valley|Mokelumne River|EBMUD|Camanche Dam & Reservoir|CMN|False|True
                            San Joaquin Valley|Calaveras River|COE|New Hogan Dam & Lake|NHG|True|True
                            San Joaquin Valley|Littlejohn Creek|COE|Farmington Dam & Reservoir|FRM|True|True
                            San Joaquin Valley|Stanislaus River|USBR|New Melones Dam & Lake|NML|False|True
                            San Joaquin Valley|Stanislaus River|USBR|Tulloch Reservoir|TUL|False|True
                            San Joaquin Valley|Tuolumne River|TID|Don Pedro Dam & Lake|DNP|False|True
                            San Joaquin Valley|Merced River|MID|New Exchequer Dam, Lake McClure|EXC|False|True
                            San Joaquin Valley|Los Banos Creek|DWR|Los Banos Detention Reservoir|LBN|False|True
                            San Joaquin Valley|Burns Creek|COE|Burns Dam & Reservoir|BUR|True|True
                            San Joaquin Valley|Bear Creek|COE|Bear Dam & Reservoir|BAR|True|True
                            San Joaquin Valley|Owens Creek|COE|Owens Dam & Reservoir|OWN|True|True
                            San Joaquin Valley|Mariposa Creek|COE|Mariposa Dam & Reservoir|MAR|True|True
                            San Joaquin Valley|Chowchilla River|COE|Buchanan Dam, H.V. Eastman Lake|BUC|True|True
                            San Joaquin Valley|Fresno River|COE|Hidden Dam, Hensley Lake|HID|True|True
                            San Joaquin Valley|San Joaquin River|USBR|Friant Dam, Millerton Lake|MIL|False|True
                            San Joaquin Valley|Big Dry Creek|FMFCD|Big Dry Creek Dam & Reservoir|BDC|False|True
                            Tulare Lake Basin|Kings River|COE|Pine Flat Dam & Lake|PNF|True|True
                            Tulare Lake Basin|Kaweah River|COE|Terminus Dam, Lake Kaweah|TRM|True|True
                            Tulare Lake Basin|Tule River|COE|Success Dam & Lake|SCC|True|True
                            Tulare Lake Basin|Kern River|COE|Isabella Dam & Lake Isabella|ISB|True|True
                            North Coast Area|Russian River|COE|Coyote Valley Dam, Lake Mendocino|COY|True|True
                            North Coast Area|Russian River|COE|Warm Springs Dam, Lake Sonoma|WRS|True|True
                            North Coast Area|Alameda Creek|DWR|Del Valle Dam & Reservoir|DLV|False|True
                            Truckee River Basin|Martis Creek|COE|Martis Creek Dam & Lake|MRT|True|True
                            Truckee River Basin|Prosser Creek|USBR|Prosser Creek Dam & Reservoir|PRS|False|True
                            Truckee River Basin|LittleTruckee River|USBR|Stampede Dam & Reservoir|STP|False|True
                            Truckee River Basin|LittleTruckee River|USBR|Boca Dam & Reservoir|BOC|False|True""")
    return pd.read_csv(csv_data, header=0, delimiter='|', index_col='WCDS_ID')


start_time = datetime.strptime('2015-08-01', '%Y-%m-%d')
end_time = datetime.strptime('2016-04-06', '%Y-%m-%d')

test = get_wcds_data('fol', start_time, end_time, 'd')







