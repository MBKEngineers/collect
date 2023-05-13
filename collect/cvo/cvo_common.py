"""
collect.cvo.common
==========================================================================
Functions that are used throughout the cvo scripts

Some will have multiple args to differentiate between the CVO data that is being read
"""
# -*- coding: utf-8 -*-
import datetime as dt

from colors import color
import pandas as pd
import numpy as np

# Remove false positive warning about editing dataframes
pd.options.mode.chained_assignment = None  # default='warn'


def report_type_maker(date, report_type):
    """
    Arguments:
        date (str): the date but in MMYY format, ex 0520
        report_type (str): specify which pdf you're looking at
    Returns:
        url (str): the url for requesting the file
    """
    if report_type == 'dout':
        prn = dt.date(2010, 12, 1)
        txt = dt.date(2002, 4, 1)
        file_date = dt.date(2000 + int(date[2:4]), int(date[0:2]), 1)
        
        # if date is before including 2010 December, give the prn link
        if txt < file_date <= prn:
            return f'https://www.usbr.gov/mp/cvo/vungvari/{report_type}{date}.prn'

        # if date is before including 2002 April, give the txt link
        elif file_date <= txt:
            return f'https://www.usbr.gov/mp/cvo/vungvari/{report_type}{date}.txt'

    return f'https://www.usbr.gov/mp/cvo/vungvari/{report_type}{date}.pdf'


def months_between(start_date, end_date):
    """
    Given two instances of ``datetime.datetime``, generate a list of dates on
    the 1st of every month between the two dates (inclusive).

    Arguments:
        start_date (datetime.datetime):  start date given by user input
        end_date (datetime.datetime): end date given by user input
    Yields:
       (datetime.date): all dates between the date range in mmyy format
    """
    if start_date > end_date:
        raise ValueError(f'Start date {start_date} is not before end date {end_date}')

    year = start_date.year
    month = start_date.month

    while (year, month) <= (end_date.year, end_date.month):
        yield dt.date(year, month, 1)

        # Move to the next month.  If we're at the end of the year, wrap around
        # to the start of the next.
        #
        # Example: Nov 2017
        #       -> Dec 2017 (month += 1)
        #       -> Jan 2018 (end of year, month = 1, year += 1)
        #
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1
            

def load_pdf_to_dataframe(ls, report_type):
    """
    changes dataframe to an array and reshape it column names change to what is specified below

    Arguments:
        ls (dataframe): dataframe in a [1, rows, columns] structure
        report_type (str): name of report
    Returns:
        df (pandas.DataFrame):
    """
    # establishing column names
    column_names = {

        'kesdop': ['Date', 'elev', 'storage', 'change', 'inflow',
                   'spring_release', 'shasta_release', 'power', 
                   'spill', 'fishtrap', 'evap_cfs'],

        'shadop': ['Date', 'elev', 'in_lake', 'change', 'inflow_cfs', 
                   'power', 'spill', 'outlet', 'evap_cfs', 'evap_in', 
                   'precip_in'],

        'shafln': ['Date', 'britton', 'mccloud', 'iron_canyon', 'pit6',
                   'pit7', 'res_total', 'd_af', 'd_cfs', 'shasta_inf', 
                   'nat_river', 'accum_full_1000af'],

        'dout': ['Date', 'SactoR_pd', 'SRTP_pd', 'Yolo_pd', 'East_side_stream_pd', 
                 'Joaquin_pd', 'Joaquin_7dy','Joaquin_mth', 'total_delta_inflow', 
                 'NDCU', 'CLT', 'TRA', 'CCC', 'BBID', 'NBA', 'total_delta_exports', 
                 '3_dy_avg_TRA_CLT', 'NDOI_daily','outflow_7_dy_avg', 
                 'outflow_mnth_avg', 'exf_inf_daily','exf_inf_3dy', 'exf_inf_14dy'],

        'fedslu':  ['Day', 'Elev', 'Storage', 'Change', 
                    'Federal Pump', 'Federal Gen', 'Pacheco Pump', 'ADJ', 
                    'Federal Change', 'Federal Storage'],

        # slunit_col_names = [
            # '__DAY',
            # '_AQUEDUCT CHECK 12_STATE',
            # '_AQUEDUCT CHECK 12_FED',
            # '_AQUEDUCT CHECK 12_TOTAL',
            # 'ONEILL_PUMPING_FED',
            # 'ONEILL_PUMPING_STATE',
            # 'ONEILL_PUMPING_TOTAL',
            # 'ONEILL_GENER_TOTAL',
            # 'SAN LUIS_PUMPING_FED',
            # 'SAN LUIS_PUMPING_STATE',
            # 'SAN LUIS_PUMPING_TOTAL',
            # 'SAN LUIS_GENERATION_FED',
            # 'SAN LUIS_GENERATION_STATE',
            # 'SAN LUIS_GENERATION_TOTAL',
            # '_PACHECO_PUMP',
            # '_DOS AMIGOS_FED',
            # '_DOS AMIGOS_STATE',
            # '_DOS AMIGOS_TOTAL',
        # ]


        'slunit': [
            '__DAY',
            '_AQUEDUCT CHECK 12_STATE',
            '_AQUEDUCT CHECK 12_FED',
            '_AQUEDUCT CHECK 12_TOTAL',
            'ONEILL_PUMPING_FED',
            'ONEILL_PUMPING_STATE',
            'ONEILL_PUMPING_TOTAL',
            'ONEILL_GENER_TOTAL',
            'SAN LUIS_PUMPING_FED',
            'SAN LUIS_PUMPING_STATE',
            'SAN LUIS_PUMPING_TOTAL',
            'SAN LUIS_GENERATION_FED',
            'SAN LUIS_GENERATION_STATE',
            'SAN LUIS_GENERATION_TOTAL',
            '_PACHECO_PUMP',
            '_DOS AMIGOS_FED',
            '_DOS AMIGOS_STATE',
            '_DOS AMIGOS_TOTAL',

            '_AQEDUCT CHECK 21_FED',
            '_AQEDUCT CHECK 21_STATE',
            '_AQEDUCT CHECK 21_TOTAL',
        ]

        # 'slunit': [
        #     '__DAY',
        #     '_AQUEDUCT CHECK 12_STATE_1',
        #     '_AQUEDUCT CHECK 12_FED_2',
        #     '_AQUEDUCT CHECK 12_TOTAL_3',
        #     'ONEILL_PUMPING_FED_4',
        #     'ONEILL_PUMPING_STATE_5',
        #     'ONEILL_PUMPING_TOTAL_6',
        #     'ONEILL_GENER_TOTAL_7',
        #     'SAN LUIS_PUMPING_FED_8',
        #     'SAN LUIS_PUMPING_STATE_9',
        #     'SAN LUIS_PUMPING_TOTAL_10',
        #     'SAN LUIS_GENERATION_FED_11',
        #     'SAN LUIS_GENERATION_STATE_12',
        #     'SAN LUIS_GENERATION_TOTAL_13',
        #     'PACHECO_PACHECO_PUMP_14',
        #     '_DOS AMIGOS_XVC_15',
        #     '_DOS AMIGOS_FED_16',
        #     '_DOS AMIGOS_STATE_17',
        #     '_DOS AMIGOS_TOTAL_18',
        #     '_AQEDUCT CHECK 21_FED_19',
        #     '_AQEDUCT CHECK 21_STATE_20',
        #     '_AQEDUCT CHECK 21_TOTAL_21'
        # ],

    }

    # remove all commas in number formatting
    df = ls[0].replace(',', '', regex=True)

    # filter so that only Day rows are included
    df = df.loc[df[0].astype(str).str.isnumeric()]

    new_obj = []
    for column, series in df.iteritems():
        test = series.astype(str).str.split()

        if len(test[0]) > 1:

            x, y = zip(*test)
            new_obj.append(pd.Series(x))
            new_obj.append(pd.Series(y))

        elif not series.dropna().empty:
            new_obj.append(series)

    nf = pd.DataFrame(new_obj).T
    nf.columns = pd.RangeIndex(start=0, stop=len(nf.columns), step=1)

    # update the column names
    nf.columns = column_names.get(report_type)

    return nf.dropna().reindex().astype(float)


def validate_user_date(date_text):
    """
    Checks if users date is in valid format
    Arguments:
        date_text (user input): date of user input
    Returns:
        None: if user input is not datetime the process will end
    """

    #if condition returns True, then nothing happens:
    assert isinstance(date_text, dt.datetime) == True , 'Please give in datetime format'


def data_cleaner(df, report_type):
    """
    This function converts data from string to floats and
    removes any non-numeric elements 

    Two seperate conditions for dout and kesdop,shadop,shafln

    Arguements:
        dataframe that was retreieved from file_getter function

    Returns:
        dataframe of strings converted to floats for data analysis
    """
    if report_type == 'dout':
        for key, value in df.iteritems():
            value = value.astype(str)
            value = value.replace(to_replace = r'[,\/]',value = '', regex =True)
            value = value.replace(to_replace = r'[%\/]',value = '', regex =True)
            value = value.replace(to_replace = r'[(\/]',value = '', regex =True)
            value = value.replace(to_replace = r'[)\/]',value = '', regex =True)

            df.loc[:,key] = value.astype(float)

        tuples = (('delta_inflow','SactoR_pd'),('delta_inflow','SRTP_pd'),('delta_inflow','Yolo_pd'),
            ('delta_inflow','East_side_stream_pd'),('delta_inflow','Joaquin_pd'),('delta_inflow','Joaquin_7dy'),
            ('delta_inflow','Joaquin_mth'),('delta_inflow','total_delta_inflow'),('NDCU','NDCU'),
            ('delta_exports','CLT'),('delta_exports','TRA'),('delta_exports','CCC'),('delta_exports','BBID'),
            ('delta_exports','NBA'),('delta_exports','total_delta_exports'),('delta_exports','3_dy_avg_TRA_CLT'),
            ('outflow_index','NDOI_daily'),('outflow_index','outflow_7_dy_avg'),('outflow_index','outflow_mnth_avg'),
            ('exp_inf','exf_inf_daily'),('exp_inf','exf_inf_3dy'),('exp_inf','exf_inf_14dy'))
        df.columns = pd.MultiIndex.from_tuples(tuples)
        
        return df

    else:
        for key, value in df.iteritems():
            value = value.astype(str)
            value = value.replace(to_replace = r'[,\/]',value = '', regex =True)
            value = value.replace(to_replace = r'[%\/]',value = '', regex =True)

            df.loc[:,key] = value.astype(float)

        if report_type == 'shafln':
            tuples = (('Storage_AF','britton'),('Storage_AF','mccloud'),('Storage_AF','iron_canyon'),
                        ('Storage_AF','pit6'),('Storage_AF','pit7'),
                        ('Res','res_total'),
                        ('change','d_af'),('change','d_cfs'),
                        ('Shasta_inflow','shasta_inf'),
                        ('Nat_river','nat_river'),
                        ('accum_full_1000af','accum_full_1000af'))
            df.columns = pd.MultiIndex.from_tuples(tuples)

        if report_type == 'fedslu':
            tuples = [
                ('Day', 'Day'),
                ('Elev', 'Elev'), 
                ('Storage', 'Storage'), 
                ('Change', 'Change'), 
                ('Federal Pump', 'Federal Pump'), 
                ('Federal Gen', 'Federal Gen'), 
                ('Pacheco Pump', 'Pacheco Pump'), 
                ('ADJ', 'ADJ'), 
                ('Federal Change', 'Federal Change'), 
                ('Federal Storage', 'Federal Storage')
            ]
            df.columns = pd.MultiIndex.from_tuples(tuples)

        if report_type == 'slunit':
            tuples = [
                ('DAY', 'DAY', 'DAY'),
                ('AQUEDUCT CHECK 12', 'AQUEDUCT CHECK 12', 'STATE'),
                ('AQUEDUCT CHECK 12', 'AQUEDUCT CHECK 12', 'FED'),
                ('AQUEDUCT CHECK 12', 'AQUEDUCT CHECK 12', 'TOTAL'),
                ('ONEILL', 'PUMPING', 'FED'),
                ('ONEILL', 'PUMPING', 'STATE'),
                ('ONEILL', 'PUMPING', 'TOTAL'),
                ('ONEILL', 'GENER', 'TOTAL'),
                ('SAN LUIS', 'PUMPING', 'FED'),
                ('SAN LUIS', 'PUMPING', 'STATE'),
                ('SAN LUIS', 'PUMPING', 'TOTAL'),
                ('SAN LUIS', 'GENERATION', 'FED'),
                ('SAN LUIS', 'GENERATION', 'STATE'),
                ('SAN LUIS', 'GENERATION', 'TOTAL'),
                ('PACHECO', 'PACHECO', 'PUMP'),
                ('DOS AMIGOS', 'DOS AMIGOS', 'FED'),
                ('DOS AMIGOS', 'DOS AMIGOS', 'STATE'),
                ('DOS AMIGOS', 'DOS AMIGOS', 'TOTAL'),

                # added for Jun2012-Dec2013
                ('AQEDUCT CHECK 21', 'AQEDUCT CHECK 21', 'FED'),
                ('AQEDUCT CHECK 21', 'AQEDUCT CHECK 21', 'STATE'),
                ('AQEDUCT CHECK 21', 'AQEDUCT CHECK 21', 'TOTAL'),

                # ('DAY', 'DAY', 'DAY', 'DAY'),
                # ('AQUEDUCT CHECK 12', 'AQUEDUCT CHECK 12', 'STATE', '1'),
                # ('AQUEDUCT CHECK 12', 'AQUEDUCT CHECK 12', 'FED', '2'),
                # ('AQUEDUCT CHECK 12', 'AQUEDUCT CHECK 12', 'TOTAL', '3'),
                # ('ONEILL', 'PUMPING', 'FED', '4'),
                # ('ONEILL', 'PUMPING', 'STATE', '5'),
                # ('ONEILL', 'PUMPING', 'TOTAL', '6'),
                # ('ONEILL', 'GENER', 'TOTAL', '7'),
                # ('SAN LUIS', 'PUMPING', 'FED', '8'),
                # ('SAN LUIS', 'PUMPING', 'STATE', '9'),
                # ('SAN LUIS', 'PUMPING', 'TOTAL', '10'),
                # ('SAN LUIS', 'GENERATION', 'FED', '11'),
                # ('SAN LUIS', 'GENERATION', 'STATE', '12'),
                # ('SAN LUIS', 'GENERATION', 'TOTAL', '13'),
                # ('PACHECO', 'PACHECO', 'PUMP', '14'),

                # ('DOS AMIGOS', 'DOS AMIGOS', 'XVC', '15'),
                # ('DOS AMIGOS', 'DOS AMIGOS', 'FED', '16'),
                # ('DOS AMIGOS', 'DOS AMIGOS', 'STATE', '17'),
                # ('DOS AMIGOS', 'DOS AMIGOS', 'TOTAL', '18'),

                # ('AQEDUCT CHECK 21', 'AQEDUCT CHECK 21', 'FED', '19'),
                # ('AQEDUCT CHECK 21', 'AQEDUCT CHECK 21', 'STATE', '20'),
                # ('AQEDUCT CHECK 21', 'AQEDUCT CHECK 21', 'TOTAL', '21'),    
            ]

            df.columns = pd.MultiIndex.from_tuples(tuples)

        elif report_type == 'kesdop':
            tuples = (('Elevation','elev'),
                        ('Storage_AF','storage'),('Storage_AF','change'),
                        ('CFS','inflow'),
                        ('Spring_release','spring_release'),
                        ('Shasta_release','shasta_release'),
                        ('Release_CFS','power'),('Release_CFS','spill'),('Release_CFS','fishtrap'),
                        ('Evap_cfs','evap_cfs'))
            df.columns = pd.MultiIndex.from_tuples(tuples)

        else:
            tuples = (('Elevation','elev'),
                        ('Storage_1000AF','in_lake'),('Storage_1000AF','change'),
                        ('CFS','inflow_cfs'),
                        ('Release_CFS','power'),('Release_CFS','spill'),('Release_CFS','outlet'),
                        ('Evaporation','evap_cfs'),('Evaporation','evap_in'),
                        ('Precip_in','precip_in'))
            df.columns = pd.MultiIndex.from_tuples(tuples)

        return df