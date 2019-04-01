# -*- coding: utf-8 -*-
# python
# water_supply_reporter
# MBK/BT
# 23JAN2014

# CN OCT 2014 - refactored to Python (not Jython)
# CN MAR 2018 - revised to use requests library
#             - revised to dictionary-based timeseries storage
#             - added command line interface with warnings

import csv
import datetime
import os
import sys
import matplotlib.pyplot as plt
from matplotlib import ticker, dates
import requests


RESERVOIRS = {
    'FOL': {
        'name': 'Folsom',
        'cnrfc': 'FOLC1',
        'usace': 'folr',
        'cdec': 'FOL',
    }, 
    'NBB': {
        'name': 'New Bullards Bar',
        'cnrfc': 'NBBC1',
        'usace': 'bulr',
        'cdec': 'BUL',
    }, 
    'ORD': {
        'name': 'Oroville',
        'cnrfc': 'ORDC1',
        'usace': 'oror',
        'cdec': 'ORO',
    }, 
    'PNF': {
        'name': 'Pine Flat',
        'cnrfc': 'PNFC1',
        'usace': 'pnfqr',
        'cdec': 'PNF',
    }, 
    'SHD': {
        'name': 'Shasta',
        'cnrfc': 'SHDC1',
        'usace': 'shar',
        'cdec': 'SHA',
    }
}


def create_plot(rfc_data, usace_data, reservoir, water_year, plot_option, results_path):
    
    plt.rcParams.update({
        'font.size': 12, 
        'figure.titlesize': 12,
        'axes.labelsize': 12,
        'legend.fontsize': 10, 
        'xtick.labelsize': 12, 
        'ytick.labelsize': 10, 
        'figure.figsize': (17, 11), 
        'legend.framealpha': 1,
        'axes.grid': True, 
        'grid.linestyle': '-', 
        'grid.color': 'lightgrey', 
        'axes.xmargin': 0, 
        'axes.linewidth': 1.0, 
        'axes.edgecolor': 'black', 
        'axes.axisbelow': True, 
        'xtick.top': False, 
        'xtick.bottom': False, 
        'xtick.minor.visible': False, 
        'ytick.right': False, 
        'ytick.left': False, 
        'ytick.minor.visible': False, 
        'lines.marker': None, 
        'lines.linewidth': 1.5, 
    })
    
    # create shared-axis plot
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(nrows=4, sharex=True)

    # precipitation bar chart
    ax1.set_ylabel('Daily Precipitation (in)')
    ax1.bar(usace_data['date'], usace_data['precip'], alpha=0.8)
    ax1.invert_yaxis()
    
    # AJ volume forecast
    ax2.set_ylabel('Forecast Apr-Jul\nRunoff Volume (TAF)')
    ax2.plot_date(rfc_data['date'], rfc_data['yval10'], "r:", label='10% exceedence')
    ax2.plot_date(rfc_data['date'], rfc_data['yval50'], "k-", label='50% exceedence')
    ax2.plot_date(rfc_data['date'], rfc_data['yval90'], "g:", label='90% exceedence')
    ax2.legend()
    
    # flow plot (convert axis to kcfs, but underlying data is cfs)
    ax3.set_ylabel('Flow (kcfs)')
    ax3.plot_date(usace_data['date'], usace_data['inflow'], "b-", label='inflow')
    ax3.plot_date(usace_data['date'], usace_data['outflow'], "g-", label='outflow')
    ax3.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, p: format(int(y / 1000.0), ',')))
    ax3.legend()

    # storage volume (convert axis to TAF but underlying data is acre-feet)
    ax4.set_ylabel('Storage (TAF)')
    ax4.plot_date(usace_data['date'], usace_data['storage'], "k-", label='storage')
    ax4.plot_date(usace_data['date'], usace_data['top_con'], "r--", label='top of conservation')
    ax4.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, p: format(int(y / 1000.0))))
    ax4.legend()
    
    # format shared x-axis
    min_time = datetime.datetime.strptime('10/01/%d'%(int(water_year)-1), '%m/%d/%Y')
    max_time = datetime.datetime.strptime('09/30/%s'%water_year, '%m/%d/%Y')
    ax4.xaxis.set_major_formatter(dates.DateFormatter(''))
    ax4.xaxis.set_major_locator(dates.MonthLocator(bymonthday=1))
    ax4.xaxis.set_minor_formatter(dates.DateFormatter('%b'))
    ax4.xaxis.set_minor_locator(dates.MonthLocator(bymonthday=15))

    ax4.set_xlim([min_time, max_time])
    plt.xticks(rotation=90)

    # set title and adjust margins
    plt.title('Seasonal Water Supply Forecast\nLocation: %s; Water Year: %s'%(
        RESERVOIRS[reservoir]['name'], water_year), y=4.75)
    fig.subplots_adjust(top=0.875, left=0.1, right=0.95)

    if plot_option == '1':
        plt.show()
    
    elif plot_option == '2':
        filename = '%s%s_%s_plt.png'%(results_path, reservoir, water_year)
        plt.savefig(filename); plt.close()
    
    elif plot_option == '3':
        filename = '%s%s_%s_plt.pdf'%(results_path, reservoir, water_year)
        print('\n    writing to %s'%filename)
        plt.savefig(filename); plt.close()


def try_float(value, missing_value):
    if value == missing_value:
        return -999    
    else:
        return float(value)


def process_data(target):

    for (reservoir, water_year, plot_option, results_path) in target:
        site_cnrfc = '?'.join(['http://www.cnrfc.noaa.gov/ensembleProductTabular.php', 
            'id=%s&prodID=7&year=%s'%(RESERVOIRS[reservoir]['cnrfc'], water_year)])
        site_usace = '?'.join(['http://www.spk-wc.usace.army.mil/fcgi-bin/getplottext.py', 
            'plot=%s&length=wy&wy=%s&interval=d'%(RESERVOIRS[reservoir]['usace'], water_year)])
       
        raw = requests.get(site_cnrfc).content
        print('\n    downloading from CNRFC')
        print('\n      processing site: %s'%site_cnrfc)

        on = False
        forecast_data = []
        for row in [str(x) for x in raw.splitlines()]:
            if on is False:
                if row.startswith('# ---'): # first occurence; set on=True
                    on = True
            else:
                row_list = row.lstrip().rstrip().split()
                if len(row_list) > 0:
                    if row_list[0][0].isdigit(): 
                        forecast_data.append(row_list)
                if row.startswith('# ---'): # second occurence; set on=False
                    on = False

        # extract exceedence series from forecast_data
        aj_forecast_data = {'date': [], 'yval10': [], 'yval50': [], 'yval90': []}
        missing_value = '<i>Missing</i>'
        for i in range(0, len(forecast_data)):
            if (forecast_data[i][1] == missing_value and forecast_data[i][2] == missing_value 
                and forecast_data[i][3] == missing_value):
                print('\tall values missing for date %s'%forecast_data[i][0])
            else:
                if len(forecast_data[i]) >= 4:
                    aj_forecast_data['date'].append(datetime.datetime.strptime(forecast_data[i][0], '%m/%d/%Y'))
                    aj_forecast_data['yval10'].append(float(forecast_data[i][1]))
                    aj_forecast_data['yval50'].append(float(forecast_data[i][2]))
                    aj_forecast_data['yval90'].append(float(forecast_data[i][3]))
                    
        # usace website processing
        raw = requests.get(site_usace).content
        print('\n    downloading from USACE-SPK Water Control Data System')
        print('\n      processing site: %s'%site_usace)

        operations_data = {'date': [], 'outflow': [], 'inflow': [], 'storage': [], 
            'top_con': [], 'precip': [], 'top_con_2': []}
        for row in [str(x) for x in raw.splitlines()]:
            if row.lstrip().startswith('2400 '):
                row_list = row[5:].lstrip().rstrip().split()
                if len(row_list) == 6:
                    operations_data['date'].append(datetime.datetime.strptime(row_list[0], '%d%b%Y'))
                    operations_data['outflow'].append(try_float(row_list[1], 'M'))
                    operations_data['inflow'].append(try_float(row_list[2], 'M'))
                    operations_data['storage'].append(try_float(row_list[3], 'M'))
                    operations_data['top_con'].append(try_float(row_list[4], 'M'))
                    operations_data['precip'].append(try_float(row_list[5], 'M'))

                if len(row_list) == 7:
                    operations_data['date'].append(datetime.datetime.strptime(row_list[0], '%d%b%Y'))
                    operations_data['outflow'].append(try_float(row_list[1], 'M'))
                    operations_data['inflow'].append(try_float(row_list[2], 'M'))
                    operations_data['top_con'].append(try_float(row_list[3], 'M'))
                    operations_data['top_con_2'].append(try_float(row_list[4], 'M'))
                    operations_data['storage'].append(try_float(row_list[5], 'M'))
                    operations_data['precip'].append(try_float(row_list[6], 'M'))
        
        # plot data
        create_plot(aj_forecast_data, operations_data, reservoir, water_year, plot_option, results_path)
        

def command_line_interface(arguments):
    """ > python py_water_supply_reporter.py FOL 2014 3 """
    
    plot_options = {'1': 'SHOW PLOT', '2': 'PNG', '3': 'PDF'}

    def _exit_cli(failed_arg='arguments'):
        print('\n'.join(['\nwater_supply_reporter:',
                         '    ---exiting---',
                         '    please supply valid %s and try again'%failed_arg]))
        sys.exit()

    def _run_process(reservoir, year, plot_option='3', results_path='out\\', warning=''):
        print('\n'.join([
            '\nwater_supply_reporter:', 
            '    reservoir   : %s'%arguments[1],
            '    year        : %s'%year + warning,
            '    plot option : %s (%s)'%(plot_option, plot_options[plot_option]),
            '    writing to  : %s\n'%(results_path),
        ]))
        process_data([(reservoir, year, plot_option, results_path)])

    def _test_reservoir(arguments):
        try:
            reservoir = RESERVOIRS[arguments[1]]
        except IndexError, KeyError:
            _exit_cli('reservoir')

    def _test_plot_option(arguments):
        try:
            plot_options[arguments[3]]
        except IndexError, KeyError:
            _exit_cli('plot option (1, 2, or 3)')

    def _test_year(arguments):
        warning = ''
        try:
            year = datetime.datetime.strptime(arguments[2], '%Y').year
            if int(year) < 2011:
                warning = '\n\t--warning--  AJ Forecast info available beginning 2011'
            if int(year) > datetime.datetime.today().year + 1:
                warning = '\n\t--warning--  No data available for future water years'
        except IndexError, ValueError:
            _exit_cli('water year')
        return warning

    if len(arguments) > 1:
        if arguments[1] in ['--list', '--reservoirs']:
            print('\nSelect 3-character reservoir code from the following:')
            print('-'*55)
            for key, value in RESERVOIRS.items():
                print '\t%s -- %s'%(key, value['name'])

        elif arguments[1] in ['--help', '--usage']:
            print('\nusage:\n\tpython py_water_supply_reporter.py [reservoir] [water year] [plotting option]\n')
            print('options and arguments:\n')
            print('\t--help        : print this help message and list options (also --usage)')
            print('\t--list        : display list of available reservoirs (also --reservoirs)\n')
            print('\n'.join(['\t[reservoir]   : a 3-character reservoir code (e.g. FOL); must be ',
                             '\t\t\ta reservoir in both USACE WCDS and CNRFC Forecasts']))
            print('\t[water year]  : 4-digit water year (AJ Forecast info begins in 2011)')
            print('\t[plot option] : one of 1 (SHOW PLOT), 2 (PNG) or 3 (PDF)')

    _test_reservoir(arguments)

    if len(arguments) == 4:
        _test_plot_option(arguments)
        _run_process(arguments[1], arguments[2], arguments[3])

    elif len(arguments) == 3:
        _run_process(arguments[1], arguments[2], warning=_test_year(arguments))

    elif len(arguments) == 2:
        _run_process(arguments[1], datetime.datetime.today().strftime('%Y'))

    else:
        _exit_cli()


if __name__ == '__main__':

    # > python py_water_supply_reporter.py FOL 2014 3
    command_line_interface(sys.argv)
