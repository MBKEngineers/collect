"""
collect.tests.test_usace
============================================================
initial test suite for collect.usace data access and utility functions; note: these tests require internet connection
"""
# -*- coding: utf-8 -*-
import datetime as dt
import unittest
from collect.usace import wcds


class TestUSACE(unittest.TestCase):

    def test_get_water_year_data(self):
        result = wcds.get_water_year_data('buc', 2021, interval='d')
        self.assertEqual(result['data'].shape, (397, 16))

        sample = result['data'].head(4)
        self.assertEqual(result['data'].head(4)['Top of Conservation (ac-ft)'].tolist(),
                         [149521.45, 149042.90, 148564.35, 148085.80])

        # does not include timezone handling
        self.assertEqual(list(map(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'), result['data'].head(4).index.tolist())),
                        ['2020-08-31 00:00:00',
                         '2020-09-01 00:00:00',
                         '2020-09-02 00:00:00',
                         '2020-09-03 00:00:00'])
          
        # does not include timezone handling
        self.assertEqual(list(map(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'), result['data'].tail(4).index.tolist())),
                        ['2021-09-28 00:00:00',
                         '2021-09-29 00:00:00',
                         '2021-09-30 00:00:00',
                         '2021-10-01 00:00:00'])

    def test_get_data(self):
        result = wcds.get_wcds_data('sha', dt.datetime(2023, 1, 15), dt.datetime(2023, 2, 1), interval='d')
        self.assertEqual(result['data'].shape, (398, 16))
        self.assertEqual(result['data']['Storage'].tolist()[:4], [1592122.0, 1590203.0, 1585627.0, 1582232.0])

    def test_get_wcds_reservoirs(self):
        """
        show that 35 reservoirs exist in the internal collect record for WCDS reservoirs
        """
        self.assertEqual(wcds.get_wcds_reservoirs().shape[0], 35)

    def test_get_wcds_data(self):
        result = wcds.get_wcds_data('sha', dt.datetime(2023, 1, 15), dt.datetime(2023, 2, 1), interval='d')
        self.assertEqual(result['data'].shape, (398, 16))
        self.assertEqual(result['data']['Storage'].tolist()[:4], [1592122.0, 1590203.0, 1585627.0, 1582232.0])

    def test_get_release_report(self):
        self.assertEqual(wcds.get_release_report('buc')['info']['units'], 'cfs')
        self.assertGreater(wcds.get_release_report('buc')['data'].shape[0], 0)

    def test_get_reservoir_metadata(self):
        result = wcds.get_reservoir_metadata('nhg', 2022, interval='d')
        self.assertEqual(int(result['gross pool (stor)']), 317100)
        self.assertEqual(int(result['gross pool (elev)']), 713)
        self.assertTrue('Precip @ Dam (in; elev 712 ft)' in result['data headers'])


if __name__ == '__main__':
    unittest.main()
