"""
collect.cvo.cvo_kesdop
============================================================
access cvo data
"""
# -*- coding: utf-8 -*-
import datetime as dt
from collect.cvo.cvo import get_data, file_getter_dout


if __name__ == '__main__':

    data = get_data(dt.datetime(2023, 5, 1), dt.datetime.now(), 'kesdop')
    print(data['data'])

    result = get_data(dt.datetime(2020, 6, 1), dt.datetime(2021, 1, 1), 'fedslu')
    result['data'].to_csv('temp/por_fedslu_2.csv')

    result = get_data(dt.datetime(2021, 1, 10), dt.datetime.now(), 'shadop')
    print(result)

    result = get_data(dt.datetime(2000, 2, 1), dt.datetime(2023, 5, 1), 'shafln')
    result['data'].to_csv('temp/por_shafln.csv')

    result = get_data(dt.datetime(2012, 6, 1), dt.datetime(2013, 12, 31), 'slunit')

    # result = get_data(dt.datetime(2012, 6, 1), dt.datetime(2013, 12, 31), 'slunit')
    # result['data'].to_csv('temp/raw_slunit_Jun2012-Dec2013.csv')

    # result = get_data(dt.datetime(2000, 1, 1), dt.datetime(2012, 5, 31))
    # result['data'].to_csv('temp/raw_slunit_Jan2000-May2012.csv')

    # r = pd.date_range(start=dt.datetime(2000, 1, 1), end=dt.datetime(2012, 5, 31), freq='D')
    # print(len(r))

    # result = get_data(dt.datetime(2014, 1, 1), dt.datetime(2016, 11, 30))
    # result['data'].to_csv('temp/raw_slunit_Jan2014-Nov2016.csv')
    # result = get_data(dt.datetime(2017, 1, 1), dt.datetime(2018, 1, 31))
    # result['data'].to_csv('temp/raw_slunit_Jan2017-Jan2018.csv')

    # result = get_data(dt.datetime(2016, 11, 1), dt.datetime(2020, 12, 31))
    # result['data'].to_csv('temp/raw_slunit_Dec2016-Dec2020.csv')

    data = file_getter_dout(dt.datetime(2022, 6, 1), dt.datetime.now())
    print(data['data'])
