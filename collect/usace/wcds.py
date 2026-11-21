"""
collect.usace.wcds
============================================================
USACE Water Control Data System (WCDS)
"""
import datetime as dt
import io
import re
import textwrap
import warnings

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
        df.index = (df.loc[:, 'Description']
                      .apply(lambda x: dt.datetime.strptime(
                            re.findall(r'Change for\s+(\d{1,2}\w{3}\d{4} @ \d{4}).*', x)[0], '%d%b%Y @ %H%M')))
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


# the first FCR report with ANY valid values for the expected tables (data ending 2400 hours 27 AUG 2012); many
# values are placeholders
_FCR_START = dt.datetime(2012, 8, 27, 0, tzinfo=utils.tz_function('US/Pacific'))

# the first report with valid values for most of each basin section's tables, by section; a section not listed
# starts at _FCR_START
_FCR_SECTION_START = {
    'Sacramento Valley': dt.datetime(2014, 2, 5, 0, tzinfo=utils.tz_function('US/Pacific')),
    'San Joaquin Valley': dt.datetime(2014, 5, 27, 0, tzinfo=utils.tz_function('US/Pacific')),
    'Tulare Lake Basin': dt.datetime(2013, 8, 19, 0, tzinfo=utils.tz_function('US/Pacific')),
    'Truckee River Basin': dt.datetime(2014, 5, 27, 0, tzinfo=utils.tz_function('US/Pacific')),
    'Russian River Basin': dt.datetime(2014, 1, 13, 0, tzinfo=utils.tz_function('US/Pacific')),
    'So. San Francisco Bay Basin': dt.datetime(2014, 5, 27, 0, tzinfo=utils.tz_function('US/Pacific')),
}


# the first report with the Folsom forecast volumes table in the Sacramento Valley section
_FCR_FOLSOM_START = dt.datetime(2019, 7, 1, 0, tzinfo=utils.tz_function('US/Pacific'))


# the basin sections of the FCR report, in report order
FCR_BASINS = ('Sacramento Valley',
              'San Joaquin Valley',
              'Tulare Lake Basin',
              'Truckee River Basin',
              'Russian River Basin',
              'So. San Francisco Bay Basin')


# section heading patterns for basins whose heading is misspelled in some reports, e.g. "---- Truckee Rivee Basin ----"
# (report data ending 23 APR 2017); other basins' headings match their name exactly
_FCR_SECTION_HEADINGS = {'Truckee River Basin': r'Truckee Rive\w Basin'}


# the basin sections of the FCR report that report no basin totals, so missing totals are expected and not warned about
_FCR_BASINS_WITHOUT_TOTALS = ('So. San Francisco Bay Basin',)


def _check_fcr_basin(basin):
    """
    raise ValueError if `basin` is not an FCR report basin section name

    Arguments:
        basin (str): the basin section name, one of FCR_BASINS, e.g. 'Sacramento Valley'
    """
    if basin not in FCR_BASINS:
        raise ValueError(f'basin must be one of {", ".join(map(repr, FCR_BASINS))}; got {basin!r}')


def _check_fcr_available(datetime_structure, table, start=_FCR_START):
    """
    whether FCR report tables are parsed for the requested date; prints a warning if not

    Arguments:
        datetime_structure (datetime.datetime): timezone-aware datetime object
        table (str): the table name for the warning, e.g. 'Sac Valley'
        start (datetime.datetime): the first supported report date; defaults to 27 AUG 2012, US/Pacific
    Returns:
        (bool): False if the requested date is before the first supported report
    """
    assert datetime_structure.tzinfo is not None, '`datetime_structure` must be timezone aware'

    if datetime_structure < start:
        print(f'WARNING: {table} table unavailable before {start:%Y-%m-%d}')
        return False
    return True


def _count_fcr_days_ago(datetime_structure):
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
        raise ValueError(f'FCR report unavailable for {datetime_structure_pacific:%Y-%m-%d}; '
                         'latest available date is yesterday (Pacific)')

    return days


def _get_fcr_report(datetime_structure, report=None):
    """
    get the full text of the USACE SPK FCR report for the requested date

    if the report has already been fetched (e.g. by `get_fcr_data`, which fetches it once for all tables) and is passed
    as `report`, it is returned as a passthrough: no request is made and the date is not checked against the report;
    otherwise the report is fetched from WCDS

    Arguments:
        datetime_structure (datetime.datetime): timezone-aware datetime object
        report (str, optional): the already-fetched report text for the requested date
    Returns:
        (str): the report text
    Raises:
        ValueError: if the report is fetched and the requested date is today or later, as the report is not yet
            available, or if the fetched report's data ending date is not the requested date (Pacific)
    """
    if report is not None:
        return report

    days = _count_fcr_days_ago(datetime_structure)

    # midnight.py `days=N` returns the report with data ending on (today - N - 1), so offset by one
    url = f'https://www.spk-wc.usace.army.mil/fcgi-bin/midnight.py?days={days - 1}&report=FCR&textonly=true'
    content = utils.get_session_response(url).text

    # the offset is relative to WCDS's current day, so confirm the report returned is for the requested date; e.g.
    # yesterday's report is not published until later in the day
    requested = datetime_structure.astimezone(utils.tz_function('US/Pacific')).date()
    report_date = _get_fcr_report_date(content)
    if report_date != requested:
        returned = 'no data ending date' if report_date is None else f'data ending {report_date:%Y-%m-%d}'
        raise ValueError(f'FCR report unavailable for {requested:%Y-%m-%d}; WCDS returned a report with {returned}, '
                         'the report may not be published yet')
    return content


def _get_fcr_report_date(content):
    """
    the data ending date of the FCR report text, from the "Data Ending 2400 hours DD MON YYYY" header

    Arguments:
        content (str): the FCR report text
    Returns:
        (datetime.date or None): the data ending date, or None if the header is not found
    """
    match = re.search(r'Data Ending 2400 hours\s+(\d{1,2} [A-Za-z]{3} \d{4})', content)
    return None if match is None else dt.datetime.strptime(match.group(1), '%d %b %Y').date()


def _get_fcr_report_generated(content):
    """
    when the FCR report text was generated, from the "Report Generated DD MON YYYY @ HHMM" header; a report for a date
    is regenerated during the following days, so this identifies which version of the report was retrieved

    Arguments:
        content (str): the FCR report text
    Returns:
        (datetime.datetime or None): the generated time in US/Pacific, or None if the header is not found
    """
    match = re.search(r'Report Generated\s+(\d{1,2} [A-Za-z]{3} \d{4}) @ (\d{4})', content)
    if match is None:
        return None
    generated = dt.datetime.strptime(f'{match.group(1)} {match.group(2)}', '%d %b %Y %H%M')
    return utils.get_localized_datetime(generated, 'US/Pacific')


def _get_fcr_section(content, basin):
    """
    the text of one basin section of the FCR report text, from its heading to the next section heading or the end of the
    report

    Arguments:
        content (str): the FCR report text
        basin (str): the basin section name, one of FCR_BASINS, e.g. 'Sacramento Valley'
    Returns:
        (str or None): the section text, or None if the section is not in the report
    """
    # section headings are centered names between dashes; the space before the closing dashes is not
    # always present (e.g. "---- Russian River Basin----")
    heading = _FCR_SECTION_HEADINGS.get(basin, re.escape(basin))
    match = re.search(rf'^\s*---- {heading} ?----\s*$([\S\s]*?)(?=^\s*---- [^\n]*?----\s*$|\Z)',
                      content.replace('\r', ''), re.MULTILINE)
    return None if match is None else match.group(0)


def _has_fcr_percent_encroached_label(content):
    """
    whether the FCR report labels the parenthesized values after the above top of conservation storage as percent
    encroached, with the "( **)" column header or the "**  Percent Encroached" footnote; earlier reports (e.g. 2014)
    show parenthesized values there without either label, and their meaning is unknown

    the label is checked for the whole report rather than each section, as the Truckee River Basin header omits "( **)"
    in reports that otherwise label the column

    Arguments:
        content (str): the FCR report text
    Returns:
        (bool): True if the report labels percent encroached
    """
    return bool(re.search(r'\(\s*\*\*\s*\)|^\s*\*\*\s+Percent Encroached', content, re.MULTILINE))


def extract_sac_valley_fcr_data(datetime_structure, nr_value=None, report=None):
    """
    get the Sacramento Valley Storages and Flood Control Parameters from the FCR report

    deprecated: use `extract_basin_fcr_table(datetime_structure, 'Sacramento Valley')`, which returns the same table

    Arguments:
        datetime_structure (datetime): datetime object
        nr_value (optional): value returned for not-reported entries, which the report marks as -NR- (the string 'NR');
            defaults to None, the same as blank and not-applicable (----, N/A) entries, so values convert to float; pass
            'NR' to keep not-reported entries distinguishable
        report (str, optional): the FCR report text for the requested date, if already retrieved (e.g. by
            `get_fcr_data`); retrieved from WCDS if not given
    Returns:
        df (pandas.DataFrame): the USACE SPK station storage and flood control parameters data, specific to Sacramento
            Valley
    """
    warnings.warn("extract_sac_valley_fcr_data is deprecated; use "
                  "extract_basin_fcr_table(datetime_structure, 'Sacramento Valley')",
                  DeprecationWarning, stacklevel=2)
    return extract_basin_fcr_table(datetime_structure, basin='Sacramento Valley', nr_value=nr_value, report=report)


def extract_folsom_fcr_data(datetime_structure, nr_value=None, report=None):
    """
    get the Folsom Storages, Flood Control Parameters, and Forecasted Volumes from the Sacramento Valley section of the
    FCR report

    deprecated: use `get_fcr_data(datetime_structure)['Sacramento Valley']['folsom']`, which returns the same table

    Arguments:
        datetime_structure (datetime): datetime object
        nr_value (optional): value returned for not-reported entries, which the report marks as -NR- (the string 'NR');
            defaults to None, the same as blank and not-applicable (----) entries, so values convert to float; pass 'NR'
            to keep not-reported entries distinguishable
        report (str, optional): the FCR report text for the requested date, if already retrieved (e.g. by
            `get_fcr_data`); retrieved from WCDS if not given
    Returns:
        df (pandas.DataFrame): the USACE SPK station storage, flood control parameters, and forecast volumes, specific
            to Folsom
    """
    warnings.warn("extract_folsom_fcr_data is deprecated; use "
                  "get_fcr_data(datetime_structure)['Sacramento Valley']['folsom']",
                  DeprecationWarning, stacklevel=2)
    return _get_fcr_folsom_forecast(datetime_structure, nr_value=nr_value, report=report)


def _get_fcr_folsom_forecast(datetime_structure, nr_value=None, report=None):
    """
    get the Folsom forecast volumes table of the FCR report for the requested date, if the date is on or after the first
    report with the table

    Arguments:
        datetime_structure (datetime): datetime object
        nr_value (optional): value returned for not-reported (-NR-) entries
        report (str, optional): the FCR report text for the requested date, if already retrieved
    Returns:
        df (pandas.DataFrame): the Folsom forecast volumes by forecast date and time; None if the date is before the
            first report with the table, or the table is not reported
    """
    if not _check_fcr_available(datetime_structure, 'Folsom forecast volumes', start=_FCR_FOLSOM_START):
        return None
    section = _get_fcr_section(_get_fcr_report(datetime_structure, report), 'Sacramento Valley')
    if section is None:
        print(f'WARNING: Sacramento Valley section not found in FCR report')
        return None
    return _parse_fcr_folsom_forecast(section, nr_value=nr_value)


def _parse_fcr_folsom_forecast(section, nr_value=None):
    """
    parse the Folsom forecast volumes table of the Sacramento Valley section of the FCR report, between the Folsom row
    ("Forecasted Volumes****") and the basin totals; the storages and percent encroached are aligned with the Sacramento
    Valley table's columns and are assigned by the positions of its rule line (see `_get_fcr_column_edges`), so a blank
    storage does not shift later values; the forecast volumes follow the above top of conservation column, separated by
    ';'

    Arguments:
        section (str): the Sacramento Valley section text
        nr_value (optional): value returned for not-reported (-NR-) entries
    Returns:
        df (pandas.DataFrame): the Folsom storages, percent encroached, and 1-, 2-, 3-, and 5-day forecast volumes by
            forecast date and time (e.g. "19NOV2026 12z"); None if the section has no forecast rows
    """
    match = re.search(r'Forecasted Volumes\*\*\*\*([\S\s]*?)BASIN TOTALS', section)
    if match is None:
        print(f'WARNING: Folsom forecast data not reported or format not supported')
        return None
    table_query = match.group(1)

    edges = _get_fcr_column_edges(section.replace('\r', '').splitlines())
    if edges is None:
        print(f'WARNING: Folsom forecast data format not supported')
        return None
    _, right_edges, above_start = edges
    # the forecast volumes begin at the first value after the above top of conservation column; a parenthesized percent
    # encroached can extend past the column (e.g. "(-142)") and is not a forecast volume
    forecast_start = right_edges[4]
    token_pattern = re.compile(rf'{_FCR_PARENTHESIZED_PATTERN}|[^\s(]+')

    # parse each forecast row independently so a single malformed, truncated, or not-reported (-NR-) row does not
    # invalidate the table; separator, "No Forecast", and blank rows do not match and are skipped
    row_pattern = re.compile(r'^\s*(\d{2}[A-Z]{3}\d{4})\s+(\d{1,2})(?:\s(.*))?$')

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
        # a row with only a date and time has no values and is kept with all values None; a truncated row (e.g.
        # "22DEC2025 18  560,378") has storages only
        forecast_date, forecast_time = match.group(1), match.group(2)
        split = next((token.start() for token in token_pattern.finditer(line, match.end(2))
                      if not token.group(0).startswith('(') and token.start() >= forecast_start), len(line))

        # storages are assigned to the top of conservation, actual, % of gross pool, and above top of conservation
        # columns by position; the row has no gross pool value
        values, percent_encroached = _split_fcr_row(line[:split], match.end(2), right_edges[:5], above_start)
        if values[0] is not None:
            print(f'WARNING: Folsom forecast value {values[0]!r} is in the gross pool column and was ignored: '
                  f'{line.strip()!r}')
        storages = values[1:5]

        forecasts = [x.strip() for x in line[split:].split(';') if x.strip()]
        if len(forecasts) > 4:
            print(f'WARNING: Folsom forecast row has more forecast volumes than columns; extra values were ignored: '
                  f'{line.strip()!r}')
        forecasts = (forecasts + [None] * 4)[:4]

        index.append(f'{forecast_date} {forecast_time}z')
        rows.append([_parse_fcr_value(x, nr_value) for x in storages + [percent_encroached] + forecasts])

    if len(rows) == 0:
        print(f'WARNING: Folsom forecast data not reported or format not supported')
        return None

    # columns without missing or text values (e.g. an `nr_value` placeholder) are float; others keep None and text
    df = pd.DataFrame(rows, index=index, columns=columns, dtype=object)
    return _convert_float_columns(df).replace(np.nan, None)


def extract_basin_fcr_table(datetime_structure, basin, nr_value=None, report=None):
    """
    get the reservoir storages and flood control parameters table of one basin section of the FCR report

    Arguments:
        datetime_structure (datetime): datetime object
        basin (str): the basin section name, one of FCR_BASINS, e.g. 'Sacramento Valley'
        nr_value (optional): value returned for not-reported entries, which the report marks as -NR- (the string 'NR');
            defaults to None, the same as blank and not-applicable (----, N/A) entries, so values convert to float; pass
            'NR' to keep not-reported entries distinguishable
        report (str, optional): the FCR report text for the requested date, if already retrieved (e.g. by
            `get_fcr_data`); retrieved from WCDS if not given
    Returns:
        df (pandas.DataFrame): the USACE SPK station storage and flood control parameters data by reservoir, including
            upstream reservoirs listed under their downstream reservoir (e.g. Pardee); None if the requested date is
            before the basin's first supported report, or the section is not in the report
    Raises:
        ValueError: if `basin` is not an FCR report basin section name
    """
    _check_fcr_basin(basin)
    if not _check_fcr_available(datetime_structure, basin, start=_FCR_SECTION_START.get(basin, _FCR_START)):
        return None

    content = _get_fcr_report(datetime_structure, report)
    section = _get_fcr_section(content, basin)
    if section is None:
        print(f'WARNING: {basin} section not found in FCR report')
        return None
    percent_encroached = _has_fcr_percent_encroached_label(content)

    if basin != 'Sacramento Valley':
        return _parse_fcr_basin_table(section, nr_value=nr_value, percent_encroached=percent_encroached)

    lines = section.replace('\r', '').splitlines()
    folsom = next((i for i, line in enumerate(lines) if re.match(r'^\s*\*?Folsom:', line)), None)
    if folsom is None:
        print(f'WARNING: FCR data format not supported')
        return None

    # before mid-2019, the Folsom row is followed by the Folsom w/SAFCA FC Diagram row and the upstream reservoirs (Union
    # Valley, French Mdw, Hell Hole), and the table ends at the line of underscores like other basins
    if 'Forecasted Volumes' not in lines[folsom]:
        return _parse_fcr_basin_table(section, nr_value=nr_value, percent_encroached=percent_encroached)

    # since mid-2019, the reservoir table ends at the Folsom row, followed by the Folsom forecast volumes table
    return _parse_fcr_basin_table('\n'.join(lines[:folsom + 1]),
                                  nr_value=nr_value,
                                  percent_encroached=percent_encroached)


def _convert_float_columns(df):
    """
    convert each column to float where all of its values allow it; columns with text values (e.g. an `nr_value`
    placeholder such as 'NR') are left as object

    Arguments:
        df (pandas.DataFrame): the parsed table, with object columns
    Returns:
        df (pandas.DataFrame): the table with float columns where possible
    """
    def convert(column):
        try:
            return column.astype(float)
        except (TypeError, ValueError):
            return column
    return df.apply(convert)


# a parenthesized FCR report value, e.g. a percent encroached "(  0)", "(-142)", or "(-NR-)", matched whole (including
# non-numeric contents) so its contents are never placed in another column; converted by `_parse_fcr_value`
_FCR_PARENTHESIZED_PATTERN = r'\([^()\n]*\)'

# an FCR report value: a parenthesized value (see _FCR_PARENTHESIZED_PATTERN); an unfilled placeholder
# starting with "%", e.g. "%NEXTCS"; a comma-grouped number; or any other value, which ends before a "(" or
# "%"; placeholders and comma-grouped numbers are matched separately so values printed without a space
# between them (e.g. "7,186,300%BBSTCS" or "10,469,90010,439,068") keep their own positions
_FCR_VALUE_PATTERN = re.compile(rf'{_FCR_PARENTHESIZED_PATTERN}|%[^\s(%]*|-?\d{{1,3}}(?:,\d{{3}})+|[^\s(%]+')


def _get_fcr_column_edges(lines):
    """
    the column positions of an FCR report basin section, from the rule line of dashes under the column headers

    values are right-aligned and wide values can extend left past their column's rule, so a value belongs to the first
    column whose rule ends at or after the value's last character

    Arguments:
        lines (list): lines of the basin section text
    Returns:
        (tuple or None): the index of the rule line; the right edges of the gross pool, top of conservation, actual, %
            of gross pool, above top of conservation, rain, and snow columns; and the start of the above top of
            conservation column; None if the rule line is not found
    """
    start = next((i for i, line in enumerate(lines) if re.match(r'^\s*-{10,}\s+-{5,}', line)), None)
    spans = None if start is None else [(m.start(), m.end()) for m in re.finditer(r'-+', lines[start])]
    if spans is None or len(spans) != 7:
        return None

    # the flood control parameters column holds right-aligned rain and snow values, so it is split at its midpoint
    params_start, params_end = spans[6]
    right_edges = [column_end for _, column_end in spans[1:6]] + [(params_start + params_end) // 2, params_end]
    return start, right_edges, spans[5][0]


def _split_fcr_row(line, start, right_edges, above_start, read_percent_encroached=True, warn_beyond=True):
    """
    assign the values in a row of an FCR report table to columns by position; prints a warning for a value that cannot
    be placed, rather than placing it in another column: two values in one column (the last is kept), or a value beyond
    the last column (ignored)

    Arguments:
        line (str): the table row
        start (int): position where the values begin, after the row label
        right_edges (list): right edge position of each column, in order; values beyond the last are ignored
        above_start (int): start position of the above top of conservation column
        read_percent_encroached (bool): whether to read the parenthesized value after the above top of conservation
            value as the percent encroached
        warn_beyond (bool): whether to warn for values beyond the last column; False where they are expected and
            ignored (e.g. the basin totals, which report storages only)
    Returns:
        values (list): the raw text of each column's value, or None if blank
        percent_encroached (str or None): the parenthesized percent encroached
    """
    # blank out labels that share the row with values (Shasta's "CFS" and, before mid-2019, the Folsom w/SAFCA FC
    # Diagram row's "ACFT" flood control parameter units, both read in the rain column; the Folsom row's "Forecasted
    # Volumes****" table heading), keeping the positions of the values
    line_values = re.sub(r'CFS|ACFT|Forecasted Volumes\*+', lambda label: ' ' * len(label.group(0)), line)

    values, percent_encroached = [None] * len(right_edges), None
    for token in _FCR_VALUE_PATTERN.finditer(line_values, start):
        if token.group(0).startswith('('):
            # the percent encroached follows the above top of conservation value; a parenthesized value
            # elsewhere in the row (e.g. after Del Valle's % of gross pool) is not a table column
            if read_percent_encroached and token.end() > above_start:
                percent_encroached = token.group(0)
            continue
        column = next((i for i, edge in enumerate(right_edges) if token.end() <= edge), None)
        if column is None:
            if warn_beyond:
                print(f'WARNING: FCR value {token.group(0)!r} is beyond the last column and was ignored: '
                      f'{line.strip()!r}')
            continue
        if values[column] is not None:
            print(f'WARNING: FCR values {values[column]!r} and {token.group(0)!r} are in the same column; '
                  f'{values[column]!r} was ignored: {line.strip()!r}')
        values[column] = token.group(0)
    return values, percent_encroached


def _parse_fcr_basin_table(section, nr_value=None, percent_encroached=True):
    """
    parse the reservoir storages and flood control parameters table of an FCR report basin section, from the rule line
    of dashes under the column headers to the line of underscores above the basin totals; values are assigned to columns
    by the positions of the rule line (see `_get_fcr_column_edges`), which keeps values in place when a column is blank
    in some or all rows (e.g. upstream reservoirs without a Top of Conservation, or a table with one row)

    Arguments:
        section (str): the basin section text
        nr_value (optional): value returned for not-reported (-NR-) entries
        percent_encroached (bool): whether to read the parenthesized value after the above top of conservation storage
            as the percent encroached; if False, the % Encroached column is None
    Returns:
        result (pandas.DataFrame): storage and flood control parameters by reservoir; None if table format not supported
            or the table has no reservoir rows with values
    """
    lines = section.replace('\r', '').splitlines()
    columns = _get_fcr_column_edges(lines)
    if columns is None:
        print(f'WARNING: FCR data format not supported')
        return None
    start, right_edges, above_start = columns
    # the line of underscores above the basin totals is searched for after the rule line, so a line of underscores
    # above the column headers does not end the table
    end = next((i for i in range(start + 1, len(lines)) if lines[i].startswith(' ___')), len(lines))

    names, rows = [], []
    for line in lines[start + 1:end]:
        match = re.match(r'^\s*\*?([A-Za-z][\w /.]*?):', line)
        if match is None:
            # rows without values (e.g. blank lines) are skipped silently; a row with values but no recognized
            # reservoir label is reported rather than dropped silently
            if re.search(r'\d', line):
                print(f'WARNING: FCR row not recognized as a reservoir and was ignored: {line.strip()!r}')
            continue
        values, percent = _split_fcr_row(line, match.end(), right_edges, above_start, percent_encroached)
        names.append(match.group(1))
        rows.append([_parse_fcr_value(x, nr_value) for x in values[:5] + [percent] + values[5:]])

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
    df = df.dropna(how='all')
    if df.empty:
        print(f'WARNING: FCR data not reported or format not supported')
        return None

    # columns without missing or text values (e.g. an `nr_value` placeholder) are float, others keep None and text
    return _convert_float_columns(df).replace(np.nan, None)


def _parse_fcr_basin_totals(section, nr_value=None, percent_encroached=True):
    """
    parse the basin totals rows of an FCR report basin section (BASIN TOTALS, TOTAL FLOOD SPACE ENCROACHED, and w/US
    Storages, as present); values are assigned to columns by the positions of the section's rule line, as for the
    reservoir table, so blank columns (e.g. the top of conservation of w/US Storages) stay in place

    Arguments:
        section (str): the basin section text
        nr_value (optional): value returned for not-reported (-NR-) entries
        percent_encroached (bool): whether to read the parenthesized value after the above top of conservation storage
            as the percent encroached; if False, the Percent Encroached column is None
    Returns:
        df (pandas.DataFrame): the basin totals by row; None if the section has no basin totals or the table format is
            not supported
    """
    lines = section.replace('\r', '').splitlines()
    columns = _get_fcr_column_edges(lines)
    if columns is None:
        return None
    start, right_edges, above_start = columns

    index, rows = [], []
    for label in ('BASIN TOTALS', 'TOTAL FLOOD SPACE ENCROACHED', 'w/US Storages'):
        line = next((line for line in lines[start + 1:] if line.startswith(f' {label}')), None)
        if line is None:
            continue
        # the totals report storages only; values beyond the above top of conservation column (e.g. a trailing
        # value in the flood control parameters column of 2014 reports) are ignored
        values, percent = _split_fcr_row(line, len(label) + 1, right_edges[:5], above_start, percent_encroached,
                                         warn_beyond=False)
        index.append(label)
        rows.append([_parse_fcr_value(x, nr_value) for x in values + [percent]])

    if len(rows) == 0:
        return None

    columns = ['Gross Pool (acft)',
               'Top of Conservation (acft)',
               'Actual Res (acft)',
               r'% of GrossPool',
               'Above Top of Conservation (acft)',
               'Percent Encroached']
    # columns without missing or text values (e.g. an `nr_value` placeholder) are float; others keep None and text
    df = pd.DataFrame(rows, index=pd.Index(index, name='Metric'), columns=columns, dtype=object)
    return _convert_float_columns(df).replace(np.nan, None)


def _parse_fcr_value(value, nr_value=None):
    """
    convert an FCR report value to float, keeping its sign; not reported (-NR-) values are returned as `nr_value`, and
    not applicable (----, N/A), blank, or non-numeric values are returned as None, including unfilled report
    placeholders such as codes ending in SP (e.g. "NHSP", "PFSP"), unfilled template variables (e.g. "%NEXTCS"), and
    "****"

    Arguments:
        value (str or number): the raw value text, which may include thousands separators and parentheses, or a value
            already read as a number
        nr_value (optional): value returned for not-reported (-NR-) entries
    Returns:
        (float, None, or nr_value): the numeric value
    """
    if isinstance(value, (int, float, np.number)):
        return None if pd.isna(value) else float(value)
    if not isinstance(value, str):
        return None
    # any whitespace (spaces, tabs) may pad the value inside or outside its parentheses
    text = value.replace(',', '').strip().strip('()').strip()
    if text == '-NR-':
        return nr_value
    try:
        return float(text)
    except ValueError:
        return None


def extract_basin_totals(datetime_structure, basin, nr_value=None, report=None):
    """
    get the basin FCR totals for one basin section of the FCR report

    Arguments:
        datetime_structure (datetime): datetime object
        basin (str): the basin section name, one of FCR_BASINS, e.g. 'Sacramento Valley'
        nr_value (optional): value returned for not-reported entries, which the report marks as -NR- (the string 'NR');
            defaults to None, the same as blank and not-applicable (----) entries, so values convert to float; pass 'NR'
            to keep not-reported entries distinguishable
        report (str, optional): the FCR report text for the requested date, if already retrieved (e.g. by
            `get_fcr_data`); retrieved from WCDS if not given
    Returns:
        df (pandas.DataFrame): the USACE SPK basin totals, with rows BASIN TOTALS, TOTAL FLOOD SPACE ENCROACHED, and
            w/US Storages, as reported for the basin (Truckee River and Russian River report no w/US Storages, and
            reports before late 2014 no TOTAL FLOOD SPACE ENCROACHED)
        OR
        None: if the requested date is before the basin's first supported report, or the basin has no totals (e.g. So.
            San Francisco Bay Basin)
    Raises:
        ValueError: if `basin` is not an FCR report basin section name
    """
    _check_fcr_basin(basin)
    if not _check_fcr_available(datetime_structure, f'{basin} basin totals',
                          start=_FCR_SECTION_START.get(basin, _FCR_START)):
        return None

    content = _get_fcr_report(datetime_structure, report)
    section = _get_fcr_section(content, basin)
    if section is None:
        print(f'WARNING: {basin} section not found in FCR report')
        return None

    df = _parse_fcr_basin_totals(section, nr_value=nr_value,
                                 percent_encroached=_has_fcr_percent_encroached_label(content))
    if df is None and basin not in _FCR_BASINS_WITHOUT_TOTALS:
        print(f'WARNING: {basin} basin totals not found in FCR report')
    return df


def extract_basin_footnotes(datetime_structure, basin, report=None):
    """
    get the footnotes of one basin section of the FCR report, as an unparsed block of text

    Arguments:
        datetime_structure (datetime): datetime object
        basin (str): the basin section name, one of FCR_BASINS, e.g. 'Sacramento Valley'
        report (str, optional): the FCR report text for the requested date, if already retrieved (e.g. by
            `get_fcr_data`); retrieved from WCDS if not given
    Returns:
        (str or None): the footnotes text, from the first footnote (e.g. " **  Percent Encroached") to the end of the
            section, including continuation lines; None if the requested date is before the basin's first supported
            report, the section is not in the report, or the section has no footnotes (e.g. Tulare Lake Basin)
    Raises:
        ValueError: if `basin` is not an FCR report basin section name
    """
    _check_fcr_basin(basin)
    if not _check_fcr_available(datetime_structure, f'{basin} footnotes',
                                start=_FCR_SECTION_START.get(basin, _FCR_START)):
        return None

    section = _get_fcr_section(_get_fcr_report(datetime_structure, report), basin)
    if section is None:
        print(f'WARNING: {basin} section not found in FCR report')
        return None
    return _parse_fcr_basin_footnotes(section)


def _parse_fcr_basin_footnotes(section):
    """
    the footnotes of an FCR report basin section, from the first footnote line to the end of the section, unparsed

    footnote lines begin with their asterisk marker followed by whitespace (e.g. " *** For a definition"), which tells
    them apart from a marked reservoir row (e.g. "*Folsom:") and the column header markers (e.g. "(acft)***")

    Arguments:
        section (str): the basin section text
    Returns:
        (str or None): the footnotes text with trailing blank lines removed; None if the section has no footnotes
    """
    lines = section.replace('\r', '').splitlines()
    first = next((i for i, line in enumerate(lines) if re.match(r'^\s*\*+\s', line)), None)
    if first is None:
        return None
    return '\n'.join(lines[first:]).rstrip()


def get_fcr_data(datetime_structure, nr_value=None):
    """
    get all of the FCR report tables for the requested date, by basin section: each basin's reservoir table, basin
    totals, and footnotes, and for Sacramento Valley, the Folsom forecast volumes; the report is retrieved once for all
    tables

    Arguments:
        datetime_structure (datetime): datetime object
        nr_value (optional): value returned for not-reported entries, which the report marks as -NR- (the string 'NR');
            defaults to None, the same as blank and not-applicable (----) entries, so values convert to float; pass 'NR'
            to keep not-reported entries distinguishable
    Returns:
        (dict): 'date' (the report date, YYYY-MM-DD in US/Pacific), 'generated' (when the report was generated, a
            US/Pacific datetime, or None if not found; a report is regenerated during the following days, so the same
            date can return different versions), and each basin section in FCR_BASINS by name (e.g. 'Sacramento
            Valley'), with 'fcr' (the reservoir table, see `extract_basin_fcr_table`), 'totals' (the basin totals, see
            `extract_basin_totals`), and 'footnotes' (the footnotes as unparsed text, see `extract_basin_footnotes`);
            'Sacramento Valley' also has 'folsom' (the Folsom forecast volumes); a table that fails to parse or is
            unavailable for the date is None, so the remaining tables are still returned
    Raises:
        ValueError: if the requested date is today or later, as the report is not yet available, or if the report
            returned is not for the requested date (Pacific)
    """
    # raise for unavailable dates up front, rather than reporting each table as failed
    _count_fcr_days_ago(datetime_structure)
    report = _get_fcr_report(datetime_structure)

    # the report date, which is in Pacific
    date = f'{datetime_structure.astimezone(utils.tz_function("US/Pacific")):%Y-%m-%d}'

    def extract(name, function, **kwargs):
        try:
            return function(datetime_structure, report=report, **kwargs)
        except Exception as error:
            print(f'WARNING: FCR `{name}` table could not be parsed for {date}: {type(error).__name__}: {error}')
            return None

    result = {'date': date, 'generated': _get_fcr_report_generated(report)}
    for basin in FCR_BASINS:
        result[basin] = {'fcr': extract(f'{basin} fcr', extract_basin_fcr_table, basin=basin, nr_value=nr_value),
                         'totals': extract(f'{basin} totals', extract_basin_totals, basin=basin, nr_value=nr_value),
                         'footnotes': extract(f'{basin} footnotes', extract_basin_footnotes, basin=basin)}
    result['Sacramento Valley']['folsom'] = extract('Sacramento Valley folsom', _get_fcr_folsom_forecast,
                                                    nr_value=nr_value)
    return result
