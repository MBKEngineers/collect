# -*- coding: utf-8 -*-
import datetime
import requests
from pprint import pprint


def floatify(value):
    try:
        return float(value)
    except ValueError:
        return value


def b120_scraper():
    url = 'http://cdec4gov.water.ca.gov/cgi-progs/iodir/B120'
    result = requests.get(url).text

    update_header = result[result.find('<h2>B120 ('):result.find('</h2>') + 5]
    last_update = datetime.datetime.strptime(update_header, '<h2>B120 (%m/%d/%y %H%M)</h2>')

    forecast_of_unimpaired_runoff_table = result[result.find('-' * 80) + 80:result.find(
        'Water-Year (WY) Forecast and Monthly Distribution')]

    water_year_forecast_and_monthly_distribution = result[result.find(
        'Water-Year (WY) Forecast and Monthly Distribution'):result.find('</pre>')]

    rivers = {
        # NORTH COAST
        # 'Trinity River at Lewiston Lake': 'LLKC1',
        # 'Scott River near Fort Jones': 'FTJC1',
        # SACRAMENTO RIVER
        # 'Sacramento River above Shasta Lake': None,
        # 'McCloud River above Shasta Lake': 'MSSC1',
        # 'Pit River above Shasta Lake': 'PITC1',
        'Total inflow to Shasta Lake': 'SHDC1',
        'Sacramento River above Bend Bridge': 'BDBC1',
        'Feather River at Oroville': 'ORDC1',
        # 'Yuba River at Smartsville': None,
        'American River below Folsom Lake': 'FOLC1',
        # SAN JOAQUIN RIVER
        'Cosumnes River at Michigan Bar': 'MHBC1',
        'Mokelumne River inflow to Pardee': 'CMPC1',
        'Stanislaus River below Goodwin Res.': 'NMSC1',
        'Tuolumne River below La Grange': 'NDPC1',
        'Merced River below Merced Falls': 'EXQC1',
        'San Joaquin River inflow to Millerton Lk': 'FRAC1',
        # TULARE LAKE
        'Kings River below Pine Flat Res.': 'PFTC1',
        'Kaweah River below Terminus Res.': 'TMDC1',
        'Tule River below Lake Success': 'SCSC1',
        'Kern River inflow to Lake Isabella': 'ISAC1',
        # NORTH LAHONTAN
        # 'Truckee River,Tahoe to Farad accretions': 'FARC1',
        # 'Lake Tahoe Rise, in feet': 'TAHC1',
        # 'West Carson River at Woodfords': 'WOOC1',
        # 'East Carson River near Gardnerville': 'GRDN2',
        # 'West Walker River below Little Walker': 'WWBC1',
        # 'East Walker River near Bridgeport': 'BPRC1',
    }

    result = {'Last Update': last_update}
    for line in forecast_of_unimpaired_runoff_table.split('\n'):       

        if any(line.strip().startswith(river) for river in rivers.keys()):

            try:
                aj_median, percent_of_avg, aj_90, dash, aj_10 = [floatify(x) for x in line.split()[-5:]]

                river = line.strip().split('   ')[0]

                result.update({
                    rivers[river]: {
                        50: aj_median, 
                        'forecast_as_%_of_avg': percent_of_avg, 
                        90: aj_90, 
                        10: aj_10
                    }
                })
            except ValueError:
                print(line)

    return result


def b120_monthly_forecasts():
    url = 'http://cdec4gov.water.ca.gov/cgi-progs/iodir/B120'
    result = requests.get(url).text

    last_update = datetime.datetime.strptime(
        result[result.find('<h2>B120 ('):result.find('</h2>') + 5],
        '<h2>B120 (%m/%d/%y %H%M)</h2>'
    )

    table_name = 'Water-Year (WY) Forecast and Monthly Distribution'

    data = result[result[result.find(table_name):].find('-' * 80) + 80:result.find('Notes:')]

    rivers = [
        'Trinity,  Lewiston', 
        'Inflow to Shasta', 
        'Sacramento, Bend', 
        'Feather, Oroville', 
        'Yuba, Smartville', 
        'American, Folsom', 
        'Cosumnes, Mich.Bar', 
        'Mokelumne, Pardee', 
        'Stanislaus, Gdw.', 
        'Tuolumne, LaGrange', 
        'Merced, McClure', 
        'San Joaquin, Mil.', 
        'Kings, Pine Flat', 
        'Kaweah, Terminus', 
        'Tule, Success', 
        'Kern, Isabella', 
    ]

    headers = ['Oct-Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 
        'WY Total', '90% Exceedance','10% Exceedance','WY %Avg']

    result = {'Last Updated': last_update}
    for line in data.splitlines():
        if any(river in line[:18] for river in rivers):
            result.update({line[:18].strip(): dict((x, y) 
                for (x, y) in zip(headers, [floatify(x) 
                for x in line[18:].split() if x!= '-']))})

    return result


def b120_update_scraper(year, month):
    """
    WATER SUPPLY FORECAST UPDATE
    YYYY April-July Unimpaired Runoff    (1,000 Acre-feet)
    """
    month = str(month).rjust(2, '0')
    url = 'http://cdec.water.ca.gov/cgi-progs/iodir/B120UP.{0}{1}'.format(year, month)
    result = requests.get(url).text

    update_header = result[result.find('<h2>B120UP.{0}{1}'.format(year, month)):result.find('</h2>') + 5]
    last_update = datetime.datetime.strptime(
        update_header,
        '<h2>B120UP.{0}{1} (%m/%d/%y %H%M)</h2>'.format(year, month)
    )

    m = datetime.datetime.strptime(month, '%m').strftime('%b')

    header = result[result.find('(1,000 Acre-feet)')+17:result.find('-' * 80)].strip().replace('{0} '.format(m), '{0}_'.format(m))

    forecast_update_headers = ['{0}_{1}'.format(header.split()[i-1], x)  
        if 'Avg' in x else x
        for i, x in enumerate(header.split())]

    forecast_update_table = result[result.find('-' * 80) + 80:result.find('Questions ')]

    exc_90 = []
    exc_50 = []
    exc_10 = []
    for line in forecast_update_table.splitlines():
        if '90% E' in line: 
            z = list(zip(forecast_update_headers, [floatify(x) for x in line[15:].split()]))
            exc_90.append(dict((x, y) for (x, y) in z))

        elif '50% E' in line: 
            z = list(zip(forecast_update_headers, [floatify(x) for x in line[15:].split()]))
            exc_50.append(dict((x, y) for (x, y) in z))

        elif '10% E' in line: 
            z = list(zip(forecast_update_headers, [floatify(x) for x in line[15:].split()]))
            exc_10.append(dict((x, y) for (x, y) in z))

    rivers = [
        'Shasta Lake, Total Inflow', 
        'Sacramento River, above Bend Bridge (near Red Bluff)', 
        'Feather River at Oroville', 
        'Yuba River near Smartsville', 
        'American River, below Folsom Lake', 
        'Mokelumne River, Inflow to Pardee Reservoir', 
        'Stanislaus River, below Goodwin Res. (blw New Melones)', 
        'Tuolumne River, below La Grange Res. (blw Don Pedro)', 
        'Merced River, below Merced Falls (blw Lake McClure)', 
        'San Joaquin River, below Millerton Lake', 
        'Kings River, below Pine Flat Reservoir', 
        'Kaweah River, below Terminus Reservoir', 
        'Tule River, below Lake Success', 
        'Kern River, inflow to Isabella Lake', 
    ]

    result = {}
    for i, (x, y, z) in enumerate(zip(exc_90, exc_50, exc_10)):
        result.update({rivers[i]: {'90': x, '50': y, '10': z}})

    return result


if __name__ == '__main__':
    # b120_update_scraper(2017, 2)

    pprint(b120_monthly_forecasts())