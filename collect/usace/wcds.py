# -*- coding: utf-8 -*-
import datetime as dt
import io

from bs4 import BeautifulSoup
import pandas as pd
import requests
from dateutil.parser import parse


def format_float(value):
    try:
        return float(value)
    except ValueError:
        return None


def get_water_year(datetime_structure):
    """
    Returns water year of current datetime object.
    -------------------------|---------------|----------------------------
    argument                 | type          |  example
    -------------------------|---------------|----------------------------
        datetime_structure   |  dt.datetime  |  dt.datetime(2016, 10, 1)
    -------------------------|---------------|----------------------------
    """
    
    YEAR = datetime_structure.year
    if datetime_structure.month < 10:
        return YEAR
    else:
        return YEAR + 1


def get_water_year_data(reservoir, water_year, interval='d'):
    """
    Scrape water year operations data from Folsom entry on USACE-SPK's WCDS.
    -----------------|---------------|----------------------------
    argument         | type          |  example
    -----------------|---------------|----------------------------
        reservoir    |  str          |  'fol'
        water_year   |  int          |  2017
        interval     |  str          |  'd'
    -----------------|---------------|----------------------------
    

    Note: times formatted as 2400 are assigned to 0000 of the next date. (hourly and daily)
    """
    result = []

    # USACE-SPK Folsom page
    url = '&'.join(['http://www.spk-wc.usace.army.mil/fcgi-bin/getplottext.py?plot={reservoir}r', 
        'length=wy', 
        'wy={water_year}', 
        'interval={interval}']).format(reservoir=reservoir,
                                       water_year=water_year,
                                       interval=interval)
    
    # reservoir data series
    series_map = get_data_columns(reservoir, water_year)

    # Download url content and parse HTML
    soup = BeautifulSoup(requests.get(url).content , 'lxml')

    header = []

    # Parse Folsom reservoir page for date/time of interest
    for i, line in enumerate(soup.find('pre').text.splitlines()):
        
        if 3 <= i <= 5:
            header.append(parse_header(line, reservoir))

        row = line.split()
        if check_date_row(row):

            entry = {
                'datestring': parse_date(row[0], row[1]), 
            }

            for key, value in series_map.items():
                entry.update({key: format_float(row[value])})

            result.append(entry)
        else:
            pass

    df = pd.DataFrame.from_records(result, index='datestring')

    return {'data': df, 'info': {'reservoir': reservoir, 
                                 'interval': interval, 
                                 'water year': water_year}}


def parse_date(time_string, date_string):
    """
    Python hours are indexed 0-23, but WCDS posts 0100-2400
    """
    hours = time_string[:2]
    minutes = time_string[2:]
    return parse(date_string) + dt.timedelta(hours=int(hours)) + dt.timedelta(minutes=int(minutes))


def parse_header(line, reservoir):

    for breaker in ['FLOW', 'TOP', 'PRECIP', 'STOR-RES', '@', reservoir.upper()]:
        line = line.replace(breaker, '|'+breaker)

    return [x.strip() for x in line.split('|')]


def check_date_row(row):
    if not bool(row):
        return False
    elif len(row) < 2:
        return False
    try:
        dt.datetime.strptime(row[1], '%d%b%Y')
        return True
    except:
        return False


def get_data_columns(reservoir, water_year=None):
    """
    TO DO: add columns mapping for non San Joaquin basin reservoirs
    """

    result = {'FLOW-RES IN': 2, 
              'FLOW-RES OUT': 3}

    if reservoir.lower() in ['sha', 'blbq', 'bul', 'oro', 'inv', 'cmn', 'nml', 'tul', 'dnp', 'exc', 'lbn', 'bucq', 'hidq', 'mil', 'dlv', 'mrtq', 'prs', 'stp', 'boc']:
        result.update({'TOP CON STOR': 5, 
                       'STOR-RES EOP': 4})

    if reservoir.lower() == 'inv':
        result.update({ 'PRECIP-INC': 6})
        if (water_year >= 2009)&(water_year != 2014):
            result.update({ 'YCFCWCA TOP CON STOR': 6,
                            'PRECIP-INC': 7})
        if (water_year >= 2015):
            result.update({'TOP CON STOR': 4,
                            'YCFCWCA TOP CON STOR': 5,
                            'STOR-RES EOP': 6, 
                            'PRECIP-INC': 7})

    if reservoir.lower() in ['bucq', 'hidq', 'mil', 'sha', 'bul', 'blbq', 'oro', 'nml', 'tul', 'dnp', 'exc', 'dlv', 'mrtq', 'boc']:
        result.update({'PRECIP-INC': 6})

    if reservoir.lower() == 'prs':
        if water_year <= 2006:
            result.update({'PRECIP-INC': 6})

    if reservoir.lower() == 'stp':
        if water_year <= 2005:
            result.update({'PRECIP-INC': 6})

    if reservoir.lower() == 'hidq':
        if water_year >= 2009:
            result.update({'ABV HENSLEY FLOW': 7})

    if reservoir.lower() in ['burq', 'ownq', 'barq', 'marq', 'bdc', 'engq']:
        result.update({'STOR-RES EOP': 4, 
                       'PRECIP-INC': 5})

    if reservoir.lower() in ['bul', 'sha', 'blbq','oro', 'cmn', 'nml', 'tul', 'dnp', 'exc', 'lbn', 'bucq', 'hidq', 'mil', 'dlv', 'mrtq', 'prs', 'stp', 'boc']:
        if water_year >= 2015:
            result.update({'TOP CON STOR': 4, 
                           'STOR-RES EOP': 5})

    if reservoir.lower() == 'barq':
        result.update({'AT MCKEE RD FLOW': 6, 
                       'BLK RASCAL D FLOW': 7})
   
    if reservoir.lower() == 'nml': 
        if water_year >= 2017:
            result.update({'FLOW-RES OUT': 2, 
                           'FLOW-RES IN': 3, 
                           'NMLLATE TOP CON STOR': 5, 
                           'STOR-RES EOP': 6, 
                           'TOP CON STOR': 4, 
                           'PRECIP-INC': 7})

    if reservoir.lower() == 'bdc': 
        result.update({'BIG DRY CR FLOW': 6, 
                       'LITTLE @BDC FLOW': 7,
                       'WASTEWAY FLOW': 8})

    if reservoir.lower() == 'pnfq':
        result.update({'BLW NF KINGS FLOW': 5, 
                       'NR PIEDRA FLOW': 6,
                       'TOP CON STOR': 7, 
                       'STOR-RES EOP': 4, 
                       'PRECIP-INC': 8})
        if water_year >= 2015:
            result.update({'BLW NF KINGS FLOW': 4, 
                           'NR PIEDRA FLOW': 5,
                           'TOP CON STOR': 6, 
                           'STOR-RES EOP': 7, 
                           'PRECIP-INC': 8})

    if reservoir.lower() == 'oro':
        if water_year >= 2016:
            result.update({'THERMOLITO FLOW': 4, 
                           'TOP CON STOR': 5,
                           'STOR-RES EOP': 6, 
                           'PRECIP-BASIN': 7})

    if reservoir.lower() == 'fol':
        result.update({'FLOW-RES OUT': 2, 
                       'FLOW-RES IN': 3, 
                       'TOP CON STOR': 4, 
                       'SAFCA TOP CON STOR': 5, 
                       'STOR-RES EOP': 6, 
                       'PRECIP-BASIN': 7})

    if reservoir.lower() == 'folq':
        result.update({'FLOW-RES OUT': 2, 
                       'FLOW-RES IN': 3, 
                       '@LAKE NATOMA FLOW-RESOUT': 4,
                       '@FAIR OAKS MISSING FLOW': 5,
                       'TOP CON STOR': 6, 
                       'SAFCA TOP CON STOR': 7, 
                       'FBO TOP CON STOR': 8, 
                       'STOR-RES EOP': 9, 
                       'PRECIP-BASIN': 10})
    
    if reservoir.lower() == 'nhgq':
        result.update({'STOR-RES EOP': 4,
                       'FLOW': 5,
                       'TOP CON STOR': 6,
                       'PRECIP-BASIN': 7})
        if water_year >= 1996:
            result.update({'STOR-RES EOP': 4,
                           'FLOW': 5,
                           'BELLOTA FLOW': 6,
                           'TOP CON STOR': 7,
                           'PRECIP-BASIN': 8})
        if water_year >= 2015:
            result.update({'STOR-RES EOP': 7,
                           'FLOW': 4,
                           'BELLOTA FLOW': 5,
                           'TOP CON STOR': 6,
                           'PRECIP-BASIN': 8})
    
    if reservoir.lower() == 'frmq':
        if water_year != 2014:
            result.update({'STOR-RES EOP': 4,
                           'PRECIP-INC': 5,
                           'FLOW AT FARMINGTON': 6,
                           'FLOW NR FARMINGTON': 7,
                           'FLOW DUCK CR DIV': 8})
        if water_year >= 1999:
            result.update({'FLOW BLW FARMINGTON': 9})
        if water_year == 2014:
            result.update({'FLOW DUCK CR DIV': 4,
                           'PRECIP-INC': 5,
                           'FLOW AT FARMINGTON': 6,
                           'FLOW BLW FARMINGTON': 7})

    if reservoir.lower() == 'trmq':
        result.update({'STOR-RES EOP': 4, 
                       'FLOW AT THREE RIV': 5, 
                       'FLOW NR LEMONCOVE': 6,
                       'TOP CON STOR': 7,
                       'PRECIP-BASIN': 8})
        if water_year >= 2015:
            result.update({'FLOW AT THREE RIV': 4,
                           'FLOW NR LEMONCOVE': 5,
                           'TOP CON STOR': 6,
                           'STOR-RES EOP': 7,
                           'PRECIP-BASIN': 8})

    if reservoir.lower() == 'sccq':
        result.update({'STOR-RES EOP': 4, 
                       'FLOW NR SPRINGVIL': 5, 
                       'FLOW NR SUCCESS': 6,
                       'TOP CON STOR': 7,
                       'PRECIP-BASIN': 8})
        if water_year >= 2015:
            result.update({'FLOW NR SPRINGVIL': 4,
                           'FLOW NR SUCCESS': 5,
                           'TOP CON STOR': 6,
                           'STOR-RES EOP': 7,
                           'PRECIP-BASIN': 8})

    if reservoir.lower() == 'isbq':
        if water_year != 2017:
            result.update({'STOR-RES EOP': 4, 
                           'FLOW AT KERNVILLE': 5, 
                           'FLOW BOREL CANAL': 6,
                           'FLOW': 7,
                           'TOP CON STOR': 8,
                           'PRECIP-BASIN': 9})
            if water_year >= 2015:
                result.update({'FLOW AT KERNVILLE': 4,
                               'FLOW BOREL CANAL': 5,
                               'FLOW': 6,
                               'TOP CON STOR': 7,
                               'STOR-RES EOP': 8,
                               'PRECIP-BASIN': 9})
        if water_year == 2017:
            result.update({'FLOW': 4,
                           'TOP CON STOR': 5,
                           'STOR-RES EOP': 6,
                           'PRECIP-BASIN': 7})

    if reservoir.lower() == 'coyq':
        result.update({'FLOW-RES OUT': 2, 
                       'FLOW-RES IN': 3, 
                       'STOR-RES EOP': 4,
                       'FLOW NR UKIAH': 5,
                       'FLOW NR HOPLAND': 6,
                       'TOP CON STOR': 7,
                       'PRECIP-INC': 8})
        if water_year >= 2009:
            result.update({'TOP CON STOR COYHIGH': 7,
                           'TOP CON STOR': 8,
                           'PRECIP-INC': 9})
        if water_year >= 2015:
            result.update({'FLOW NR UKIAH': 4,
                           'FLOW NR HOPLAND': 5,
                           'TOP CON STOR COYHIGH': 6,
                           'TOP CON STOR': 7,
                           'STOR-RES EOP': 8,
                           'PRECIP-INC': 9})

    if reservoir.lower() == 'wrsq':
        result.update({'STOR-RES EOP': 4,
                       'FLOW NR GUERNEVIL': 5,
                       'FLOW NR GEYSERVIL': 6,
                       'TOP CON STOR': 7,
                       'PRECIP-INC': 8})
        if water_year >= 1996:
            result.update({'FLOW NR HEALDSBUR (CDEC)': 5,
                       'FLOW NR GUERNEVIL': 6,
                       'FLOW NR GEYSERVIL': 7,
                       'TOP CON STOR': 8,
                       'PRECIP-INC': 9})


    return result


def get_wcds_data(reservoir, start_time, end_time, interval='d'):
    """
    Scrape water year operations data from reservoir page on USACE-SPK's WCDS.
    -----------------|---------------|----------------------------
    argument         | type          |  example
    -----------------|---------------|----------------------------
        reservoir    |  str          |  'fol'
        start_time   |  dt.datetime  |  dt.datetime(2016, 10, 1)
        end_time     |  dt.datetime  |  dt.datetime(2017, 11, 5)
        interval     |  str          |  'd'
    -----------------|---------------|----------------------------
    """
    frames = []
    for water_year in range(get_water_year(start_time), get_water_year(end_time) + 1):
        frames.append(get_water_year_data(reservoir, water_year, interval)['data'])

    df = pd.concat(frames)
    if interval == 'd':
        df.index = pd.to_datetime(df.index, format='2400 %d%b%Y')
    else:
        df.index = pd.to_datetime(df.index, format='%H%M %d%b%Y')

    return {'data': df, 'info': {'reservoir': reservoir, 
                                 'interval': interval, 
                                 'notes': 'daily data value occurs on midnight of entry date'}}


def get_wcds_reservoirs():
    """
    Corps and Section 7 Projects in California
    http://www.spk-wc.usace.army.mil/plots/california.html
    """
    csv_data = io.StringIO("""Region|River Basin|Agency|Project|WCDS_ID|Hourly Data|Daily Data
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