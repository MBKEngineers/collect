"""
collect.usace.wcds
============================================================
USACE Water Control Data System (WCDS)
"""
# -*- coding: utf-8 -*-
import datetime as dt
import io
import re
import textwrap

import numpy as np
import pandas as pd
import requests
import ssl

from collect import utils


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
    response = requests.get(url, verify=ssl.CERT_NONE).content
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

    # add a day to timesteps where 24T is in the index (TODO: when numpy <1.26 include format='mixed')
    new_index = pd.Series(pd.to_datetime(df.index.str.replace('T24:', ' ')), index=df.index)
    mask = df.index.str.contains('T24:')
    new_index[mask] += pd.Timedelta(days=1)

    # create datetime index in US/Pacific time to match WCDS
    df.index = pd.to_datetime(new_index.values, utc=True).tz_convert('US/Pacific')

    # Define variable for reservoir metadata
    metadata_dict = get_reservoir_metadata(reservoir, water_year, interval)
    
    return {'data': df, 
            'info': {'reservoir': reservoir,
                     'water year': water_year,
                     'interval': interval,
                     'metadata': metadata_dict}}


def get_data(reservoir, start_time, end_time, interval='d', clean_column_headers=True):
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
    # reservoir code is case-sensitive
    reservoir = reservoir.lower()

    # Check that user-chosen water year is within range with data
    earliest_time = dt.datetime.strptime('1994-10-01', '%Y-%m-%d')

    if start_time.tzname() in ['US/Pacific', 'PST', 'PDT']:
        earliest_time = start_time.tzinfo.localize(earliest_time)

    if start_time < earliest_time:
        print(f'No data for selected start date. Earliest possible start date selected instead: {earliest_time}')
        start_time = earliest_time

    # assume date/times are provided in UTC timezone if no timezone is provided
    if start_time.tzinfo is None:
        start_time = start_time.astimezone(dt.timezone.utc)
    if end_time.tzinfo is None:
        end_time = end_time.astimezone(dt.timezone.utc)

    # Make new dataframe
    frames = []
    metadata_dict = {}

    for water_year in range(utils.get_water_year(start_time), utils.get_water_year(end_time) + 1):
        result = get_water_year_data(reservoir, water_year, interval)
        truncate_result = result['data'].truncate(before=start_time, after=end_time)
        frames.append(truncate_result)
        metadata_dict.update({water_year: result['info']['metadata']})

    df = pd.concat(frames)
    df.index.name = 'ISO 8601 Date Time'

    # strip units from column headers
    if clean_column_headers:
        df.rename(_cleaned_columns_map(df.columns), axis=1, inplace=True)

    # return timeseries data and record metadata
    return {'data': df, 
            'info': {'reservoir': reservoir, 
                     'interval': interval, 
                     'notes': 'daily data value occurs on midnight of entry date',
                     'metadata': metadata_dict}}


def get_wcds_reservoirs():
    """
    Corps and Section 7 Projects in California
    http://www.spk-wc.usace.army.mil/plots/california.html

    Returns:
        (pandas.DataFrame): dataframe containing table of WCDS reservoirs
    """
    csv_data = io.StringIO(textwrap.dedent("""\
        Region|River Basin|Agency|Project|WCDS_ID|Hourly Data|Daily Data
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
        Truckee River Basin|LittleTruckee River|USBR|Boca Dam & Reservoir|BOC|False|True"""))
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
    return get_data(reservoir.lower(),
                    start_time,
                    end_time,
                    interval=interval,
                    clean_column_headers=clean_column_headers)


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
    url = f'https://www.spk-wc.usace.army.mil/fcgi-bin/release.py?project={reservoir}&textonly=true'

    # request data from url
    response = requests.get(url, verify=ssl.CERT_NONE).content
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
    """
    Retrieves website metadata from USACE-SPK's WCDS.
    
    Arguments:
        reservoir (str): three-letter reservoir code; i.e. 'fol'
        water_year (int): the water year
        interval (str): data interval; i.e. 'd' 

    Returns:
        result (dict): query result dictionary
    """
    # reservoir code is case-sensitive
    reservoir = reservoir.lower()

    # USACE-SPK Folsom page
    url = f'https://www.spk-wc.usace.army.mil/plots/csv/{reservoir}{interval}_{water_year}.meta'
    
    # read data from url using requests session with retries
    response = requests.get(url, verify=ssl.CERT_NONE)

    # complete metadata dictionary
    metadata_dict = response.json()
    result = {
        'data headers': metadata_dict['allheaders'],
        'gross pool (stor)': metadata_dict['ymarkers']['Gross Pool']['value'],
        'generated': metadata_dict['generated'],
        'datum': None
    }

    # check for changing elevation key
    if 'Gross Pool(elev NGVD29)' in metadata_dict['ymarkers']:
        result.update({'gross pool (elev)': metadata_dict['ymarkers']['Gross Pool(elev NGVD29)']['value'],
                       'datum': 'NGVD29'})
    else:
        result.update({'gross pool (elev)': metadata_dict['ymarkers']['Gross Pool(elev)']['value']})
    
    # returns relevant subset as dictionary
    return result


def extract_fcr_text(datetime_structure):
    """
    read the text in the Sacramento Valley section of the USACE SPK FCR report

    Arguments:
        datetime_structure (datetime.datetime): datetime object
    Returns:
        (list): list of text from the FCR report
    """
    now = dt.datetime.now(tz=datetime_structure.tzinfo)
    days = (now-datetime_structure).days
    url = f'https://www.spk-wc.usace.army.mil/fcgi-bin/midnight.py?days={days-1}&report=FCR&textonly=true'
    content = requests.get(url, verify=ssl.CERT_NONE).text
    # get the list of text between Sacramento Valley and San Joaquin Valley
    return re.findall(r'(?<=Sacramento Valley)[\S\s]*(?=San Joaquin Valley)', content)


def extract_sac_valley_fcr_data(datetime_structure):
    """
    get the Sacramento Valley Storages and Flood Control Parameters from the FCR report

    Arguments:
        datetime_structure (datetime): datetime object
    Returns:
        df (pandas.DataFrame): the USACE SPK station storage and flood control parameters data,
            specific to Sacramento Valley
    """
    last_date = dt.datetime(2014, 2, 5)
    if datetime_structure < last_date:
        print(f'WARNING: Sac Valley table unavailable before {last_date:%Y-%m-%d}')
        return None

    query = extract_fcr_text(datetime_structure)[0]
    table_query = re.findall(r' Shasta:[\S\s]*(?=Folsom:)', query)[0].replace('CFS', '')

    # add a space before values in parentheses so they are read as a separate column
    table_query = table_query.replace('(', '( ')

    # get the Folsom row and the Top of Conservation belonging to it
    fol_row = re.findall(r'Folsom:\s*\S+', query)[0]

    # read both tables as fixed width files
    df = pd.read_fwf(io.StringIO(table_query), header=None)
    fol_row = pd.read_fwf(io.StringIO(fol_row), header=None)

    # Combine other resevoirs with Folsom row
    df = pd.concat((df, fol_row), axis=0)

    # remove characters from text
    df = (df
         ).replace(',', '', regex=True
         ).replace('-', '', regex=True
         ).replace('', None, regex=True
         ).replace('NR', None, regex=True
         ).replace(r'\(', '', regex=True
         ).replace(r'\)', '', regex=True
         )

    df[0] = df[0].str.replace(':', '')

    if len(df.columns) != 9:
        print(f'WARNING: FCR data format not supported')
        return None

    df.columns = ['Reservoir',
                  'Gross Pool (acft)',
                  'Top of Conservation (acft)',
                  'Actual Res (acft)',
                  r'% of Gross Pool',
                  'Above top of Conservation (acft)',
                  r'% Encroached',
                  'Flood Control Parameters (Rain in.)',
                  'Flood Control Parameters (Snow acft)']

    df = df.set_index('Reservoir')

    return df.dropna(how='all').astype(float, errors='ignore').replace(np.nan, None)


def extract_folsom_fcr_data(datetime_structure):
    """
    get the Folsom Storages, Flood Control Parameters, and Forecasted Volumes from the Sacramento Valley
    section of the FCR report

    Arguments:
        datetime_structure (datetime): datetime object
    Returns:
        df (pandas.DataFrame): the USACE SPK station storage, flood control parameters, and forecasted volumes,
            specific to Folsom
    """
    last_date = dt.datetime(2019, 7, 2)
    if datetime_structure < last_date:
        print(f'WARNING: Folsom forecasted volumes table unavailable before {last_date:%Y-%m-%d}')
        return None

    query = extract_fcr_text(datetime_structure)[0]
    table_query = re.findall(r'(?=Forecasted Volumes)[\S\s]*(?=BASIN TOTALS)', query)[0].split('Forecasted Volumes****')[1]
    for symbol in ['-', '(', ')', ';', ',']:
        table_query = table_query.replace(symbol, '')

    # remove No Forecast rows or blank rows if in table
    table_query = table_query.split('No Forecast')[0].split('______')[0]

    df = pd.read_fwf(io.StringIO(table_query), header=None)

    if len(df.columns) != 11:
        print(f'WARNING: Folsom data format not supported')
        return None

    df.columns = ['Forecasted Date',
                  'Forecasted Time',
                  'Top of Conservation (acft)',
                  'Actual Res (acft)',
                  r'% of GrossPool',
                  'Above Top of Conservation(acft)',
                  'Percent Encroached',
                  '1-Day Forecasted Volume',
                  '2-Day Forecasted Volume',
                  '3-Day Forecasted Volume',
                  '5-Day Forecasted Volume'
                  ]

    df.index = df.apply(lambda x: f"{x['Forecasted Date']} {x['Forecasted Time']}z", axis=1) 
    df = df.drop(columns=['Forecasted Date', 'Forecasted Time'])

    return df.dropna(how='all').astype(float, errors='ignore').replace(np.nan, None)


def extract_basin_totals(datetime_structure):
    """
    get the Sacramento Valley basin FCR totals

    Arguments:
        datetime_structure (datetime): datetime object
    Returns:
        df (pandas.DataFrame): the USACE SPK total Sacramento Valley basin FCR values
        OR
        table_query (str): raw text for basin totals
    """
    query = extract_fcr_text(datetime_structure)[0]

    # try reading data in one of two formats
    try:
        table_query = re.findall(r' BASIN TOTALS[\S\s]*(?=Percent Encroached)', query)[0].replace('CFS', '')
    except IndexError:
        table_query = re.findall(r' BASIN TOTALS[\S\s]*(?=Folsom \(COE\) Diagram not used in Calculations)', query)[0].replace('CFS', '')

    try:
        # read fixed-width format file into pandas Dataframe
        df = pd.read_fwf(io.StringIO(table_query),
                         names=[
                            'Col0',
                            'Actual Res (acft)', r'% of GrossPool',
                            'Above Top of Conservation(acft)',
                            'Percent Encroached'
                            ]
                        )

        df['Above Top of Conservation (acft)'] = df['Above Top of Conservation(acft)'].str.replace('(', '', regex=True)
        df['Percent Encroached'] = df['Percent Encroached'].str.replace(')', '', regex=True) 

        # remove commas from values in dataframe
        df = df.replace(',', '', regex=True)
        
        df[['Metric', 'Gross Pool (acft)', 'Top of Conservation (acft)']
            ] = df['Col0'].str.extract(r'([a-zA-Z\s]+)  (\d+) (\d+)')
        df.loc[1, 'Metric'] = df.loc[1, 'Col0'] 

        rowindexer = df.index == 2
        df.loc[rowindexer, ['Metric', 'Gross Pool (acft)']
            ] = df.loc[rowindexer, 'Col0'].str.extract(r'(w/[a-zA-Z\s]+) (\d+)').values

        df = df.drop('Col0', axis=1)
        df = df[[
            'Metric',
            'Gross Pool (acft)',
            'Top of Conservation (acft)',
            'Actual Res (acft)',
            r'% of GrossPool',
            'Above Top of Conservation (acft)',
            'Percent Encroached'
        ]]
        df = df.set_index('Metric')
        df = df.replace(',', '', regex=True)

        return df.dropna(how='all').astype(float).replace(np.nan, None)

    except (KeyError, ValueError, AttributeError):
        return table_query

def get_fcr_data(datetime_structure):
    """
    get all of the FCR date for Sacramento Valley basin

    Arguments:
        datetime_structure (datetime): datetime object
    Returns:
        (dict): dictionary containing each dataframe for all FCR, Folsom specific metrics, and basin totals
    """
    return {'date': f'{datetime_structure:%Y-%m-%d}',
            'fcr': extract_sac_valley_fcr_data(datetime_structure),
            'folsom': extract_folsom_fcr_data(datetime_structure),
            'totals': extract_basin_totals(datetime_structure)}
