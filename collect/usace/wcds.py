"""
collect.usace.wcds
============================================================
USACE Water Control Data System (WCDS)
"""
import datetime as dt
import io
import re
import textwrap

import numpy as np
import pandas as pd
import requests

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
    response = requests.get(url).content
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

    # WCDS marks end-of-day as hour 24, which pandas/datetime cannot represent, so rewrite it
    # as hour 00 and add a day below. Rewriting to 'T00:' (rather than stripping the field)
    # keeps every row in one 'YYYY-MM-DDTHH:MM:SS±ZZ:ZZ' shape for the ISO8601 parser.
    # utc=True normalizes the mixed PST/PDT offsets to instants (index is converted below).
    new_index = pd.Series(pd.to_datetime(df.index.str.replace('T24:', 'T00:'), format='ISO8601', utc=True),
                          index=df.index)
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
        # supports either pytz or zoneinfo tzinfo objects on start_time
        earliest_time = utils.get_localized_datetime(earliest_time, 'US/Pacific')

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
    response = requests.get(url).content
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
    response = requests.get(url)

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


def _fcr_days_ago(datetime_structure):
    """
    the number of whole calendar days (Pacific) between today and the requested FCR report date

    Arguments:
        datetime_structure (datetime.datetime): timezone-aware datetime object
    Returns:
        days (int): days before today; at least 1
    Raises:
        ValueError: if the requested date is today or later, as the report is not yet available
    """
    assert datetime_structure.tzinfo is not None, '`datetime_structure` must be timezone aware'

    # data on WCDS is in Pacific
    datetime_structure_pacific = datetime_structure.astimezone(utils.tz_function('US/Pacific'))

    # get today's date in pacific time
    today_pacific = dt.datetime.now(tz=datetime_structure_pacific.tzinfo).date()

    # compute the whole calendar days between today and the requested date
    days = (today_pacific - datetime_structure_pacific.date()).days

    # the most recent available report is for yesterday's data
    if days < 1:
        raise ValueError(f'FCR report unavailable for {datetime_structure_pacific:%Y-%m-%d}; latest available date is yesterday (Pacific)')

    return days


def _get_fcr_report(datetime_structure):
    """
    get the full text of the USACE SPK FCR report for the requested date

    Arguments:
        datetime_structure (datetime.datetime): timezone-aware datetime object
    Returns:
        (str): the report text
    Raises:
        ValueError: if the requested date is today or later, as the report is not yet available
    """
    days = _fcr_days_ago(datetime_structure)

    # midnight.py `days=N` returns the report with data ending on (today - N - 1), so offset by one
    url = f'https://www.spk-wc.usace.army.mil/fcgi-bin/midnight.py?days={days - 1}&report=FCR&textonly=true'
    return utils.get_session_response(url).text


def extract_fcr_text(datetime_structure):
    """
    read the text in the Sacramento Valley section of the USACE SPK FCR report

    Arguments:
        datetime_structure (datetime.datetime): datetime object
    Returns:
        (list): list of text from the FCR report
    """
    content = _get_fcr_report(datetime_structure)
    # get the list of text between Sacramento Valley and San Joaquin Valley
    return re.findall(r'(?<=Sacramento Valley)[\S\s]*(?=San Joaquin Valley)', content)


def _extract_fcr_section(datetime_structure, heading):
    """
    read the text of one basin section of the USACE SPK FCR report, from its heading (e.g.
    "---- San Joaquin Valley ----") to the next section heading or the end of the report

    Arguments:
        datetime_structure (datetime.datetime): datetime object
        heading (str): the section name, e.g. 'San Joaquin Valley'
    Returns:
        (str or None): the section text, or None if the section is not in the report
    """
    return _fcr_section(_get_fcr_report(datetime_structure), heading)


def _fcr_section(content, heading):
    """
    the text of one basin section of the FCR report text, from its heading to the next section heading or the
    end of the report

    Arguments:
        content (str): the FCR report text
        heading (str): the section name, e.g. 'San Joaquin Valley'
    Returns:
        (str or None): the section text, or None if the section is not in the report
    """
    # section headings are centered names between dashes; the space before the closing dashes is not
    # always present (e.g. "---- Russian River Basin----")
    match = re.search(rf'^\s*---- {re.escape(heading)} ?----\s*$([\S\s]*?)(?=^\s*---- [^\n]*?----\s*$|\Z)',
                      content.replace('\r', ''), re.MULTILINE)
    return None if match is None else match.group(0)


def _fcr_labels_percent_encroached(content):
    """
    whether the FCR report labels the parenthesized values after the above top of conservation storage as
    percent encroached, with the "( **)" column header or the "**  Percent Encroached" footnote; earlier
    reports (e.g. 2014) show parenthesized values there without either label, and their meaning is unknown

    the label is checked for the whole report rather than each section, as the Truckee River Basin header
    omits "( **)" in reports that otherwise label the column

    Arguments:
        content (str): the FCR report text
    Returns:
        (bool): True if the report labels percent encroached
    """
    return bool(re.search(r'\(\s*\*\*\s*\)|^\s*\*\*\s+Percent Encroached', content, re.MULTILINE))


def extract_sac_valley_fcr_data(datetime_structure, nr_value=None):
    """
    get the Sacramento Valley Storages and Flood Control Parameters from the FCR report

    Arguments:
        datetime_structure (datetime): datetime object
        nr_value (optional): value returned for not-reported entries, which the report marks as -NR- (the
            string 'NR'); defaults to None, the same as blank and not-applicable (----) entries, so values
            convert to float; pass 'NR' to keep not-reported entries distinguishable
    Returns:
        df (pandas.DataFrame): the USACE SPK station storage and flood control parameters data,
            specific to Sacramento Valley
    """
    last_date = dt.datetime(2014, 2, 5, 0, tzinfo=utils.tz_function('US/Pacific'))
    if datetime_structure < last_date:
        print(f'WARNING: Sac Valley table unavailable before {last_date:%Y-%m-%d}')
        return None

    content = _get_fcr_report(datetime_structure)
    section = _fcr_section(content, 'Sacramento Valley')
    if section is None:
        print(f'WARNING: Sacramento Valley section not found in FCR report')
        return None

    # the reservoir table ends at the Folsom row, which is followed by the Folsom forecasted volumes table
    lines = section.replace('\r', '').splitlines()
    folsom = next((i for i, line in enumerate(lines) if re.match(r'^\s*\*?Folsom:', line)), None)
    if folsom is None:
        print(f'WARNING: FCR data format not supported')
        return None

    df = _parse_fcr_reservoir_table('\n'.join(lines[:folsom + 1]), nr_value=nr_value,
                                    percent_encroached=_fcr_labels_percent_encroached(content))
    if df is None:
        return None

    # columns without missing values are returned as float, as before the shared basin table parser was used
    return df.astype(float, errors='ignore').replace(np.nan, None)


def extract_folsom_fcr_data(datetime_structure, nr_value=None):
    """
    get the Folsom Storages, Flood Control Parameters, and Forecasted Volumes from the Sacramento Valley
    section of the FCR report

    Arguments:
        datetime_structure (datetime): datetime object
        nr_value (optional): value returned for not-reported entries, which the report marks as -NR- (the
            string 'NR'); defaults to None, the same as blank and not-applicable (----) entries, so values
            convert to float; pass 'NR' to keep not-reported entries distinguishable
    Returns:
        df (pandas.DataFrame): the USACE SPK station storage, flood control parameters, and forecasted volumes,
            specific to Folsom
    """
    last_date = dt.datetime(2019, 7, 1, 0, tzinfo=utils.tz_function('US/Pacific'))
    if datetime_structure <= last_date:
        print(f'WARNING: Folsom forecasted volumes table unavailable before {last_date:%Y-%m-%d}')
        return None

    query = extract_fcr_text(datetime_structure)[0]
    table_query = re.findall(r'(?=Forecasted Volumes)[\S\s]*(?=BASIN TOTALS)', query)[0].split('Forecasted Volumes****')[1]

    # parse each forecast row independently so a single malformed, truncated, or not-reported (-NR-) row
    # does not invalidate the table; separator, "No Forecast", and blank rows do not match and are skipped
    row_pattern = re.compile(r'^\s*(\d{2}[A-Z]{3}\d{4})\s+(\d{1,2})\s(.*)$')

    columns = ['Top of Conservation (acft)',
               'Actual Res (acft)',
               r'% of GrossPool',
               'Above Top of Conservation(acft)',
               'Percent Encroached',
               '1-Day Forecasted Volume',
               '2-Day Forecasted Volume',
               '3-Day Forecasted Volume',
               '5-Day Forecasted Volume']

    # rows are kept as a list because observed and forecast rows can share a timestamp
    index, rows = [], []
    for line in table_query.splitlines():
        match = row_pattern.match(line)
        if match is None:
            continue
        forecast_date, forecast_time, values = match.groups()

        # storages precede the percent encroached in parentheses; forecasted volumes follow, separated by ';'
        storages, _, remainder = values.partition('(')
        percent_encroached, _, forecasts = remainder.partition(')')
        storages = (storages.split() + [None] * 4)[:4]
        forecasts = (forecasts.split(';') + [None] * 4)[:4]

        index.append(f'{forecast_date} {forecast_time}z')
        rows.append([_parse_fcr_value(x, nr_value) for x in storages + [percent_encroached] + forecasts])

    if len(rows) == 0:
        print(f'WARNING: Folsom forecast data not reported or format not supported')
        return None

    # object dtype keeps missing values as None, consistent with the other FCR tables
    return pd.DataFrame(rows, index=index, columns=columns, dtype=object)


def extract_san_joaquin_valley_fcr_data(datetime_structure, nr_value=None):
    """
    get the San Joaquin Valley Flood Control Requirements report

    Arguments:
        datetime_structure (datetime): datetime object
        nr_value (optional): value returned for not-reported entries, which the report marks as -NR- (the
            string 'NR'); defaults to None, the same as blank and not-applicable (----, N/A) entries, so
            values convert to float; pass 'NR' to keep not-reported entries distinguishable
    Returns:
        df (pandas.DataFrame): the USACE SPK station storage and flood control parameters data,
            specific to San Joaquin Valley,
            including upstream reservoirs listed under their downstream reservoir
    """
    return _extract_basin_reservoir_data(datetime_structure, 'San Joaquin Valley', nr_value=nr_value)


def extract_tulare_lake_basin_fcr_data(datetime_structure, nr_value=None):
    """
    get the Tulare Lake Basin Flood Control Requirements report

    Arguments:
        datetime_structure (datetime): datetime object
        nr_value (optional): value returned for not-reported entries, which the report marks as -NR- (the
            string 'NR'); defaults to None, the same as blank and not-applicable (----, N/A) entries, so
            values convert to float; pass 'NR' to keep not-reported entries distinguishable
    Returns:
        df (pandas.DataFrame): the USACE SPK station storage and flood control parameters data,
            specific to Tulare Lake Basin,
            including upstream reservoirs listed under their downstream reservoir
    """
    return _extract_basin_reservoir_data(datetime_structure, 'Tulare Lake Basin', nr_value=nr_value)


def extract_truckee_river_basin_fcr_data(datetime_structure, nr_value=None):
    """
    get the Truckee River Basin Flood Control Requirements report

    Arguments:
        datetime_structure (datetime): datetime object
        nr_value (optional): value returned for not-reported entries, which the report marks as -NR- (the
            string 'NR'); defaults to None, the same as blank and not-applicable (----, N/A) entries, so
            values convert to float; pass 'NR' to keep not-reported entries distinguishable
    Returns:
        df (pandas.DataFrame): the USACE SPK station storage and flood control parameters data,
            specific to Truckee River Basin
    """
    return _extract_basin_reservoir_data(datetime_structure, 'Truckee River Basin', nr_value=nr_value)


def extract_russian_river_basin_fcr_data(datetime_structure, nr_value=None):
    """
    get the Russian River Basin Flood Control Requirements report

    Arguments:
        datetime_structure (datetime): datetime object
        nr_value (optional): value returned for not-reported entries, which the report marks as -NR- (the
            string 'NR'); defaults to None, the same as blank and not-applicable (----, N/A) entries, so
            values convert to float; pass 'NR' to keep not-reported entries distinguishable
    Returns:
        df (pandas.DataFrame): the USACE SPK station storage and flood control parameters data,
            specific to Russian River Basin
    """
    return _extract_basin_reservoir_data(datetime_structure, 'Russian River Basin', nr_value=nr_value)


def extract_so_san_francisco_bay_basin_fcr_data(datetime_structure, nr_value=None):
    """
    get the So. San Francisco Bay Basin Flood Control Requirements report

    Arguments:
        datetime_structure (datetime): datetime object
        nr_value (optional): value returned for not-reported entries, which the report marks as -NR- (the
            string 'NR'); defaults to None, the same as blank and not-applicable (----, N/A) entries, so
            values convert to float; pass 'NR' to keep not-reported entries distinguishable
    Returns:
        df (pandas.DataFrame): the USACE SPK station storage and flood control parameters data,
            specific to So. San Francisco Bay Basin
    """
    return _extract_basin_reservoir_data(datetime_structure, 'So. San Francisco Bay Basin', nr_value=nr_value)


def _extract_basin_reservoir_data(datetime_structure, heading, nr_value=None):
    """
    get the reservoir storages and flood control parameters table of one FCR report basin section

    Arguments:
        datetime_structure (datetime): datetime object
        heading (str): the section name, e.g. 'San Joaquin Valley'
        nr_value (optional): value returned for not-reported (-NR-) entries
    Returns:
        df (pandas.DataFrame): storage and flood control parameters by reservoir; None if the section is not
            in the report or its table format is not supported
    """
    content = _get_fcr_report(datetime_structure)
    section = _fcr_section(content, heading)
    if section is None:
        print(f'WARNING: {heading} section not found in FCR report')
        return None
    return _parse_fcr_reservoir_table(section, nr_value=nr_value,
                                      percent_encroached=_fcr_labels_percent_encroached(content))


# a parenthesized percent encroached, e.g. "(  0)" or "(-142)", or any other value, which ends before a "("
_FCR_VALUE_PATTERN = re.compile(r'\(\s*-?\d+\s*\)|[^\s(]+')


def _parse_fcr_reservoir_table(section, nr_value=None, percent_encroached=True):
    """
    parse the reservoir storages and flood control parameters table of an FCR report basin section, from
    the rule line of dashes under the column headers to the line of underscores above the basin totals

    values are assigned to columns by the positions of the rule line: values are right-aligned and wide
    values can extend left past their column's rule, so a value belongs to the first column whose rule
    ends at or after the value's last character; this keeps values in place when a column is blank in
    some or all rows (e.g. upstream reservoirs without a Top of Conservation, or a table with one row)

    Arguments:
        section (str): the basin section text
        nr_value (optional): value returned for not-reported (-NR-) entries
        percent_encroached (bool): whether to read the parenthesized value after the above top of
            conservation storage as the percent encroached; if False, the % Encroached column is None
    Returns:
        df (pandas.DataFrame): storage and flood control parameters by reservoir; None if the table format
            is not supported
    """
    read_percent_encroached = percent_encroached
    lines = section.replace('\r', '').splitlines()
    start = next((i for i, line in enumerate(lines) if re.match(r'^\s*-{10,}\s+-{5,}', line)), None)
    end = next((i for i, line in enumerate(lines) if line.startswith(' ___')), len(lines))
    spans = None if start is None else [(m.start(), m.end()) for m in re.finditer(r'-+', lines[start])]
    if spans is None or len(spans) != 7:
        print(f'WARNING: FCR data format not supported')
        return None

    # right edges of the gross pool through above top of conservation columns; the flood control parameters
    # column holds right-aligned rain and snow values, so it is split at its midpoint
    params_start, params_end = spans[6]
    right_edges = [column_end for _, column_end in spans[1:6]] + [(params_start + params_end) // 2, params_end]
    above_start = spans[5][0]

    names, rows = [], []
    for line in lines[start + 1:end]:
        match = re.match(r'^\s*\*?([A-Za-z][\w /.]*?):', line)
        if match is None:
            continue
        values, percent_encroached = [None] * len(right_edges), None
        for token in _FCR_VALUE_PATTERN.finditer(line.replace('CFS', '   '), match.end()):
            if token.group(0).startswith('('):
                # the percent encroached follows the above top of conservation value; a parenthesized value
                # elsewhere in the row (e.g. after Del Valle's % of gross pool) is not a table column
                if read_percent_encroached and token.end() > above_start:
                    percent_encroached = token.group(0)
                continue
            column = next((i for i, edge in enumerate(right_edges) if token.end() <= edge), None)
            if column is not None:
                values[column] = token.group(0)
        names.append(match.group(1))
        rows.append([_parse_fcr_value(x, nr_value) for x in values[:5] + [percent_encroached] + values[5:]])

    columns = ['Gross Pool (acft)',
               'Top of Conservation (acft)',
               'Actual Res (acft)',
               r'% of Gross Pool',
               'Above top of Conservation (acft)',
               r'% Encroached',
               'Flood Control Parameters (Rain in.)',
               'Flood Control Parameters (Snow acft)']
    df = pd.DataFrame(rows, index=pd.Index(names, name='Reservoir'), columns=columns, dtype=object)

    # drop group label rows without values, e.g. "EBMUD Reservoirs:"
    return df.dropna(how='all')


def _parse_fcr_value(value, nr_value=None):
    """
    convert an FCR report value to float, keeping its sign; not reported (-NR-) values are returned as
    `nr_value`, and not applicable (----), blank, or non-numeric values are returned as None

    Arguments:
        value (str or number): the raw value text, which may include thousands separators and parentheses,
            or a value already read as a number
        nr_value (optional): value returned for not-reported (-NR-) entries
    Returns:
        (float, None, or nr_value): the numeric value
    """
    if isinstance(value, (int, float, np.number)):
        return None if pd.isna(value) else float(value)
    if not isinstance(value, str):
        return None
    text = value.replace(',', '').strip('() ')
    if text == '-NR-':
        return nr_value
    try:
        return float(text)
    except ValueError:
        return None


def extract_basin_totals(datetime_structure, nr_value=None):
    """
    get the Sacramento Valley basin FCR totals

    Arguments:
        datetime_structure (datetime): datetime object
        nr_value (optional): value returned for not-reported entries, which the report marks as -NR- (the
            string 'NR'); defaults to None, the same as blank and not-applicable (----) entries, so values
            convert to float; pass 'NR' to keep not-reported entries distinguishable
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

    columns = ['Gross Pool (acft)',
               'Top of Conservation (acft)',
               'Actual Res (acft)',
               r'% of GrossPool',
               'Above Top of Conservation (acft)',
               'Percent Encroached']

    # each row reports a fixed set of columns; parsing rows by label rather than inferring a fixed-width
    # layout keeps not-reported (-NR-) values and missing rows from invalidating the table
    row_columns = {'BASIN TOTALS': ['Gross Pool (acft)',
                                    'Top of Conservation (acft)',
                                    'Actual Res (acft)',
                                    r'% of GrossPool',
                                    'Above Top of Conservation (acft)'],
                   'TOTAL FLOOD SPACE ENCROACHED': ['Above Top of Conservation (acft)'],
                   'w/US Storages': ['Gross Pool (acft)',
                                     'Actual Res (acft)',
                                     r'% of GrossPool']}

    index, rows = [], []
    for label, label_columns in row_columns.items():
        match = re.search(rf'^ {re.escape(label)}(.*)$', table_query, re.MULTILINE)
        if match is None:
            continue

        # the percent encroached follows in parentheses, e.g. "-2,713,587(  0)"
        values, _, remainder = match.group(1).partition('(')
        percent_encroached = remainder.partition(')')[0] if remainder else None

        # values fill the row's columns in order; the % of gross pool is a whole number and is left blank when
        # the actual storage is not reported, e.g. "10,469,900  -NR-  -NR-  [blank]  -NR-", so a value that
        # is not a whole number belongs to the following column
        row = dict.fromkeys(columns)
        remaining_columns = iter(label_columns)
        for value in values.split():
            for column in remaining_columns:
                if column == r'% of GrossPool' and not re.fullmatch(r'\d+', value):
                    continue
                row[column] = _parse_fcr_value(value, nr_value)
                break
        row['Percent Encroached'] = _parse_fcr_value(percent_encroached, nr_value)
        index.append(label)
        rows.append([row[column] for column in columns])

    if len(rows) == 0:
        return table_query

    # object dtype keeps missing values as None, consistent with the other FCR tables
    return pd.DataFrame(rows, index=pd.Index(index, name='Metric'), columns=columns, dtype=object)


def get_fcr_data(datetime_structure, nr_value=None):
    """
    get all of the FCR date for Sacramento Valley basin

    Arguments:
        datetime_structure (datetime): datetime object
        nr_value (optional): value returned for not-reported entries, which the report marks as -NR- (the
            string 'NR'); defaults to None, the same as blank and not-applicable (----) entries, so values
            convert to float; pass 'NR' to keep not-reported entries distinguishable
    Returns:
        (dict): dictionary containing each dataframe for all FCR, Folsom specific metrics, and basin totals;
            a table that fails to parse is None so the remaining tables are still returned
    Raises:
        ValueError: if the requested date is today or later, as the report is not yet available
    """
    # raise for unavailable dates up front, rather than reporting each table as failed
    _fcr_days_ago(datetime_structure)

    result = {'date': f'{datetime_structure:%Y-%m-%d}'}
    for key, extract in (('fcr', extract_sac_valley_fcr_data),
                         ('folsom', extract_folsom_fcr_data),
                         ('totals', extract_basin_totals)):
        try:
            result[key] = extract(datetime_structure, nr_value=nr_value)
        except Exception as error:
            print(f'WARNING: FCR `{key}` table could not be parsed for '
                  f'{datetime_structure:%Y-%m-%d}: {type(error).__name__}: {error}')
            result[key] = None
    return result
