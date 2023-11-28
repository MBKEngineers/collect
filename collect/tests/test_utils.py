"""
collect.tests.test_basics
============================================================
initial test suite for collect data access and utility functions; note: these tests require internet connection
"""
# -*- coding: utf-8 -*-
import datetime as dt
import io
import os
import textwrap
import unittest
import unittest.mock
from collect import utils


class TestUtils(unittest.TestCase):

    # def test_get_session_response(self):
    #     utils.get_session_response(url)

    # def test_get_web_status(self):
    #     utils.get_web_status(url)

    # def test_clean_fixed_width_headers(self):
    #     utils.clean_fixed_width_headers(columns)

    def test_get_water_year(self):
        self.assertEqual(utils.get_water_year(dt.datetime(2023, 5, 12)), 2023)
        self.assertEqual(utils.get_water_year(dt.datetime(2023, 11, 12)), 2024)


if __name__ == '__main__':
    unittest.main()
