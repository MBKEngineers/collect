"""
collect.tests.test_usgs
============================================================
initial test suite for collect.usgs data access and utility functions; note: these tests require internet connection
"""
# -*- coding: utf-8 -*-
import datetime as dt
import unittest
from collect import usgs


class TestUSGS(unittest.TestCase):

    def test_get_query_url(self):
        url = usgs.get_query_url(11418500, '00060', dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 5), 'instantaneous')
        expected_url = '&'.join(['https://waterservices.usgs.gov/nwis/iv/?format=json',
                                 'sites=11418500',
                                 'startDT=2023-01-01T00:00:00',
                                 'endDT=2023-01-05T00:00:00',
                                 'parameterCd=00060',
                                 'siteStatus=all'])
        self.assertEqual(url, expected_url)

    def test_get_data(self):
        result = usgs.get_data(11418500, '00060', dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 5), interval='daily')
        self.assertEqual(result['data']['00060'].tolist(), [1280.0, 341.0, 351.0, 260.0, 1790.0])
        self.assertEqual(result['data'].index.strftime('%Y-%m-%d').tolist(),
                        ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'])

    def test_get_usgs_data(self):
        result = usgs.get_usgs_data(11418500, '00060', dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 5), interval='daily')
        self.assertEqual(result['data']['00060'].tolist(), [1280.0, 341.0, 351.0, 260.0, 1790.0])
        self.assertEqual(result['data'].index.strftime('%Y-%m-%d').tolist(),
                        ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'])

    def test_get_peak_streamflow(self):
        result = usgs.get_peak_streamflow(11418500)['data'][['peak_va']]
        self.assertEqual(result.head()['peak_va'].tolist(),
                         ['14000', '6260', '7520', '10800', '2400'])
        self.assertEqual(result.head().index.strftime('%Y-%m-%d').tolist(),
                        ['1928-02-29', '1936-02-21', '1937-02-04', '1937-12-11', '1939-03-08'])


if __name__ == '__main__':
    unittest.main()
