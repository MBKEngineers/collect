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

import pandas as pd

from collect import cvo
from collect import utils


class TestCVO(unittest.TestCase):

    def test(self):
        pass

        # prn test
        result = cvo.get_data(dt.date(2000, 2, 1), dt.date(2011, 3, 31), 'doutdly')

        # pdf test
        result = cvo.get_data(dt.date(2013, 12, 1), dt.date(2014, 1, 31), 'doutdly')
        result = cvo.get_data(dt.date(2000, 2, 1), dt.date(2023, 5, 1), 'shafln')
        result = cvo.get_data(dt.date(2012, 6, 1), dt.date(2013, 12, 31), 'slunit')
        result = cvo.get_data(dt.date(2020, 6, 1), dt.date(2021, 1, 1), 'fedslu')
        result = cvo.get_data(dt.date(2021, 1, 10), dt.date.now(), 'shadop')
        result = cvo.get_data(dt.date(2023, 5, 1), dt.date.now(), 'kesdop')

    def test_get_area(self):
        cvo.get_area(date_structure, report_type)

    def test_get_data(self):
        cvo.get_data(start, end, report_type)

    def test_get_date_published(self):
        cvo.get_date_published(url, date_structure, report_type)

    def test_get_report_columns(self):
        cvo.get_report_columns(report_type, date_structure, expected_length=None, default=False)

    def test_get_report(self):
        cvo.get_report(date_structure, report_type)

    def test_get_title(self):
        cvo.get_title(report_type)

    def test_get_url(self):
        cvo.get_url(date_structure, report_type)

    def test_months_between(self):
        cvo.months_between(start_date, end_date)

    def test_doutdly_data_cleaner(self):
        cvo.doutdly_data_cleaner(content, report_type, date_structure)

    def test_load_pdf_to_dataframe(self):
        cvo.load_pdf_to_dataframe(content, date_structure, report_type, to_csv=False)

    def test_download_files(self):
        cvo.download_files(start, end, report_type, destination='.')


if __name__ == '__main__':
    unittest.main()
