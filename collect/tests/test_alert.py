"""
collect.tests.test_alert
============================================================
initial test suite for collect.alert data access and utility functions; note: these tests require internet connection
"""
# -*- coding: utf-8 -*-
import datetime as dt
import unittest
from collect import alert


class TestSacAlert(unittest.TestCase):

    def test_get_site_notes(self):
        """
        test the function for retrieving site metadata produces the expected entries
        """
        result = alert.get_site_notes('1137')
        self.assertEqual(result['site_id'], '1137')
        self.assertEqual(result['Facility ID:'], 'A31')
        self.assertEqual(result['Location:'], 'Upstream of Alpine Frost Dr. west of Bruceville Rd.')
        self.assertEqual(result['Date Installed:'], '2/6/1994')

    def test_get_data(self):
        result = alert.get_data('1137',
                                dt.datetime(2021, 3, 18, 14),
                                dt.datetime(2021, 3, 18, 20),
                                device_ids=[4],
                                ascending=True,
                                as_dataframe=True)

        # check the queried sensor values for the specified date range
        self.assertEqual(result['data']['Value'].tolist(),
                         [0.0, 0.04, 0.0, 0.04, 0.04, 0.0, 0.0, 0.04, 0.0, 0.04, 0.04, 0.04, 0.0, 0.04, 0.0])

        # check the associated date/time stamps
        self.assertEqual(result['data']['Receive'].tolist()[:4],
                         ['2021-03-18 14:00:25', '2021-03-18 14:36:20', '2021-03-18 15:00:30', '2021-03-18 15:24:21'])

    def test_get_site_sensors(self):
        """
        test the function for retrieving site metadata sensors list produces the expected number of entries
        """
        self.assertEqual(len(alert.get_site_sensors(1122)['sensors']), 7)

    def test_get_sites(self):
        """
        test the function for retrieving site list for a particular gage types returns the expected number of entries
        """
        self.assertEqual(alert.get_sites(as_dataframe=True, datatype='rain').shape, (81, 12))
        self.assertEqual(alert.get_sites(as_dataframe=True, datatype='stream').shape, (37, 10))

    def test_get_sites_from_list(self):
        """
        test the expected number of sites registered on the Sac Alert websites
        """
        self.assertEqual(alert.get_sites_from_list(as_dataframe=True, sensor_class=None).shape, (127, 4))

    def test_ustrip(self):
        self.assertEqual(alert.alert._ustrip('\u00A0'), '')

    def test_get_site_location(self):
        result = alert.get_site_location(1122)
        self.assertEqual(result['latitude'], 38.6024722)
        self.assertEqual(result['longitude'], -121.3951389)

    def test_get_query_url(self):
        url = alert.get_query_url(1137, 3, dt.datetime(2023, 1, 1), dt.datetime(2023, 2, 1))
        expected_url = '&'.join([
            'https://www.sacflood.org/export/file/?site_id=1137',
            'device_id=3',
            'mode=',
            'hours=',
            'data_start=2023-01-01%2000:00:00',
            'data_end=2023-02-01%2000:00:00',
            'tz=US%2FPacific',
            'format_datetime=%25Y-%25m-%25d+%25H%3A%25i%3A%25S',
            'mime=txt',
            'delimiter=comma'
        ])
        self.assertEqual(url, expected_url)

    def test_get_device_series(self):
        result = alert.get_device_series(1108,
                                         6,
                                         dt.datetime(2023, 11, 27),
                                         dt.datetime(2023, 11, 28),
                                         ascending=True).head(4).values.tolist()
        expected_result = [['2023-11-27 00:00:00', '2023-11-27 00:31:05', 50.38, 'ft', 'A'],
                           ['2023-11-27 00:15:00', '2023-11-27 01:31:05', 50.38, 'ft', 'A'],
                           ['2023-11-27 00:30:00', '2023-11-27 01:31:05', 50.38, 'ft', 'A'],
                           ['2023-11-27 00:45:00', '2023-11-27 01:31:05', 50.38, 'ft', 'A']]
        self.assertEqual(result, expected_result)


if __name__ == '__main__':
    unittest.main()
