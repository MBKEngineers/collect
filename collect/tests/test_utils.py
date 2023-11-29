"""
collect.tests.test_utils
============================================================
initial test suite for collect.utils utility functions; note: these tests require internet connection
"""
# -*- coding: utf-8 -*-
import datetime as dt
import io
import os
import textwrap
import unittest
import unittest.mock
import pandas as pd
import requests
from collect import utils


class TestUtils(unittest.TestCase):

    def test_get_session_response(self):
        result = utils.get_session_response('https://example.com')
        self.assertTrue('<title>Example Domain</title>' in result.text)
        self.assertTrue(isinstance(result, requests.models.Response))
        self.assertEqual(result.status_code, 200)

    def test_get_web_status(self):
        self.assertTrue(utils.get_web_status('https://example.com'))

    def test_clean_fixed_width_headers(self):
        test_headers = [
            ['Unnamed: 0_level_0', '90%', '75%', '50%', '25%', '10%'] + [f'Unnamed: {i}_level_0' for i in range (6, 10)],
            ['Unnamed: 0_level_1'] + ['Exceedance'] * 5 + ['NWS', 'Raw Obs', 'Raw Avg', 'Unnamed: 9_level_1'],
            ['Unnamed: 0_level_2'] + ['Apr-Jul'] * 8 + ['Raw Daily'],
            ['Date'] + ['Forecast'] * 3 + ['Foreacast', 'Foreacst', 'Forecast', 'To Date', 'To Date', 'Observation'],
            ['(mm/dd/YYYY)'] + ['(kaf)'] * 9
        ]
        columns = pd.MultiIndex.from_tuples(zip(*test_headers))

        expected_columns = ['Date (mm/dd/YYYY)',
                            '90% Exceedance Apr-Jul Forecast (kaf)',
                            '75% Exceedance Apr-Jul Forecast (kaf)',
                            '50% Exceedance Apr-Jul Forecast (kaf)',
                            '25% Exceedance Apr-Jul Foreacast (kaf)',
                            '10% Exceedance Apr-Jul Foreacst (kaf)',
                            'NWS Apr-Jul Forecast (kaf)',
                            'Raw Obs Apr-Jul To Date (kaf)',
                            'Raw Avg Apr-Jul To Date (kaf)',
                            'Raw Daily Observation (kaf)']

        self.assertEqual(utils.clean_fixed_width_headers(columns), expected_columns)

    def test_get_water_year(self):
        self.assertEqual(utils.get_water_year(dt.datetime(2023, 5, 12)), 2023)
        self.assertEqual(utils.get_water_year(dt.datetime(2023, 11, 12)), 2024)


if __name__ == '__main__':
    unittest.main()
