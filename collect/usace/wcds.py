"""
collect.usace.wcds
============================================================
USACE Water Control Data System (WCDS)
"""
# -*- coding: utf-8 -*-
import datetime as dt
import io
import re
import pandas as pd
import requests
from collect import utils
import json


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
    # reservoir code is case-sensitive
    reservoir = reservoir.lower()

    # USACE-SPK Folsom page
    url = f'https://www.spk-wc.usace.army.mil/plots/csv/{reservoir}{interval}_{water_year}.plot'

    # Read url data
    response = requests.get(url, verify=False).content
    df = pd.read_csv(io.StringIO(response.decode('utf-8')), header=0, na_values=['-', 'M'])

    # Check that user chosen water year is within range with data
    earliest_time = 1995

    if water_year < earliest_time:
        print(f'No data for selected water year. Earliest possible year selected: {earliest_time}')
        water_year = earliest_time

    # Clean zeros in note columns
    column_list = df.columns.tolist()
    note_columns = []

    for col in column_list:
        if col.find('notes') != -1:
            note_columns.append(col)

    df[note_columns] = df[note_columns].replace(0, float('NaN'))

    # Convert to date time object
    df.set_index('ISO 8601 Date Time', inplace=True)
    df.index = pd.to_datetime(df.index)

    result = {'data': df, 
            'info': {'reservoir': get_reservoir_metadata(reservoir, water_year, interval),
                     'water year': water_year, 
                     'interval':interval}}
    return result

def get_data(reservoir, start_time, end_time, interval='d', clean_column_headers=True):
    """
    TODO:  trim this to start and end
    Scrape water year operations data from reservoir page on USACE-SPK's WCDS.
    
    Arguments:
        reservoir (str): three-letter reservoir code
        start_time (datetime.datetime): query start datetime
        end_time (datetime.datetime): query end datetime
        interval (str): data interval
    Returns:
        result (dict): query result dictionary with data and info keys
    """
    # reservoir code is case-sensitive
    reservoir = reservoir.lower()

    # Check that user chosen water year is within range with data
    earliest_time = dt.datetime.strptime('1994-10-01', '%Y-%m-%d')

    if start_time.tzname() in ['US/Pacific', 'PST', 'PDT']:
        earliest_time = start_time.tzinfo.localize(earliest_time)

    if start_time < earliest_time:
        print(f'No data for selected start date. Earliest possible start date selected instead: {earliest_time}')
        start_time = earliest_time

    # Make new dataframe
    frames = []
    for water_year in range(utils.get_water_year(start_time), utils.get_water_year(end_time) + 1):
        frames.append(get_water_year_data(reservoir, water_year, interval)['data'])

    df = pd.concat(frames)
    df.index.name = 'ISO 8601 Date Time'

    # strip units from column headers
    if clean_column_headers:
        df.rename(_cleaned_columns_map(df.columns), axis=1, inplace=True)

    result = {'data': df, 
            'info': {'reservoir': reservoir, 
                     'interval': interval, 
                     'notes': 'daily data value occurs on midnight of entry date'}}
    # return timeseries data and record metadata
    return result


def get_wcds_reservoirs():
    """
    Corps and Section 7 Projects in California
    http://www.spk-wc.usace.army.mil/plots/california.html

    Returns:
        (pandas.DataFrame): dataframe containing table of WCDS reservoirs
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


def get_wcds_data(reservoir, start_time, end_time, interval='d', clean_column_headers=True):
    """
    alias for wcds.get_data function, to support backwards compatibility

    Arguments:
        reservoir (str): three-letter reservoir code
        start_time (datetime.datetime): query start datetime
        end_time (datetime.datetime): query end datetime
        interval (str): data interval
    Returns:
        result (dict): query result dictionary with data and info keys
    """
    return get_data(reservoir.lower(), start_time, end_time, interval=interval, clean_column_headers=clean_column_headers)


def _cleaned_columns_map(columns):
    """
    strips units and parentheses from WCDS column headers, since reservoir columns headers are not consistent
    
    Arguments:
        columns (list, array, pandas.Index, tuple): iterable of column headers
    Returns:
        (dict): map of original column names to simplified column names
    """
    return {x: re.sub(r'(\(.*\))', '', x).replace('  ', ' ').strip() for x in columns}


def get_release_report(reservoir):
    """
    download release change data reports from https://www.spk-wc.usace.army.mil/reports/release_changes.html
    each reservoir release change report is provided via email; as such, there is no one standard format
    for the timeseries of release changes; email content is provided in string format
    
    Arguments:
        reservoir (str): three-letter reservoir code, lowercase
    Returns:
        (dict): dictionary containing the release change email and other reservoir info
    """
    info = {'reservoir': reservoir, 
            'url': f'https://www.spk-wc.usace.army.mil/reports/getreport.html?report=release/rel-{reservoir}'}

    # reservoir code is case-sensitive
    reservoir = reservoir.lower()

    # USACE-SPK release report email source data
    url = f'https://www.spk-wc.usace.army.mil/reports/release/rel-{reservoir}'

    # request data from url
    response = utils.get_session_response(url).content
    raw = response.decode('utf-8')

    # check for header matching pattern with pipe delimiters
    header = re.findall(r'(\|\s+From\|\s+To\|.*\r\n)', raw)
    if len(header) > 0:
        # determine column labels from pipe-deplimited header row
        column_headers = ['Description', *[x.strip() for x in header[0].split('|')  if bool(x.strip())]]

        # extract table contents from between header and footer
        table = raw.split(header[0])[-1].split(':'*10)[0]
        df = pd.read_csv(io.StringIO(table), header=None, delimiter='|', usecols=list(range(len(column_headers))))
        df.columns = column_headers

        # create a date/time index
        df.index = df.loc[:, 'Description'].apply(lambda x: dt.datetime.strptime(re.findall(r'Change for\s+(\d{1,2}\w{3}\d{4} @ \d{4}).*', x)[0], '%d%b%Y @ %H%M'))
        df.index.name = 'Date/Time'

        # extract the email comment footer
        comment = re.findall(r':{10,60}\s+(.*\s+.*)\s+:{10,60}', raw)
        comment = ' '.join([x.strip() for x in comment[0].split('\r\n')]) if len(comment) > 0 else ''

        # generation time
        generated = re.findall(r'(Release Notification Generated \d{1,2}\w{3}\d{4} @ \d{4} hours)', raw)

        # units
        units = re.findall(r'All flows reported in (\w+).\s+', raw)

        # return release change data
        return {'data': df, 
                'info': {**info, 
                         'comment': comment,
                         'units': units[0] if len(units) > 0 else '', 
                         'generated': generated[0] if len(generated) > 0 else '', 
                         'raw': raw}}

    # default without dataframe parsing
    return {'data': raw, 'info': info}

def get_reservoir_metadata(reservoir, water_year, interval='d'):
    # reservoir code is case-sensitive
    reservoir = reservoir.lower()

    # USACE-SPK Folsom page
    url = f'https://www.spk-wc.usace.army.mil/plots/csv/{reservoir}{interval}_{water_year}.meta'
    
    # Read url data
    response = requests.get(url, verify=False)
    text = response.text
    site_info = json.loads(text)
    return site_info

