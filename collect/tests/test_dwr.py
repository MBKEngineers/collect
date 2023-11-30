"""
collect.tests.test_dwr
============================================================
initial test suite for collect.dwr data access and utility functions; note: these tests require internet connection
"""
# -*- coding: utf-8 -*-
import datetime as dt
import io
import os
import textwrap
import unittest
import unittest.mock

import pandas as pd

from collect.dwr import cdec
from collect.dwr import casgem
from collect.dwr import cawdl
from collect.dwr import b120
from collect.dwr import swp


class TestCASGEM(unittest.TestCase):
    """
    dwr.casgem module references inactive API; CASGEM tools must be updated once CNRA completes web transition
    """
    def deferred_test_get_casgem_data(self):
        return

        casgem_id_result = casgem.get_casgem_data(
            casgem_id='34318',
            state_well_number=None,
            local_well_designation=None,
            master_site_code=None,
            write_to_html_file=False
        )

        state_well_number_result = casgem.get_casgem_data(
            casgem_id=None,
            state_well_number='19N02W36H001M',
            local_well_designation=None,
            master_site_code=None,
            write_to_html_file=False
        )

        local_well_designation_result = casgem.get_casgem_data(
            casgem_id=None,
            state_well_number=None,
            local_well_designation='19N02W36H001M',
            master_site_code=None,
            write_to_html_file=False
        )

        master_site_code_result = casgem.get_casgem_data(
            casgem_id=None,
            state_well_number=None,
            local_well_designation=None,
            master_site_code='394564N1220246W001',
            write_to_html_file=False
        )


class TestCAWDL(unittest.TestCase):
    """
    dwr.cawdl module references inactive API; CAWDL tools must be updated once CNRA/DWR completes web transition
    """
    def deferred_test_get_cawdl_data(self):
        result = cawdl.get_cawdl_data('17202')

    def deferred_test_get_cawdl_surface_water_data(self):
        result = cawdl.get_cawdl_surface_water_data('17202', 2021, 'FLOW', interval='DAILY_MEAN')

    def deferred_test_get_cawdl_surface_water_por(self):
        result = cawdl.get_cawdl_surface_water_por('17202', 'FLOW', interval='DAILY_MEAN')

    def deferred_test_get_cawdl_surface_water_site_report(self):
        result = cawdl.get_cawdl_surface_water_site_report('17202')


class TestCDEC(unittest.TestCase):

    def test_get_b120_data(self):
        """
        test for B120 data-retrieval function relying on https://cdec.water.ca.gov/b120.html
        """
        result = b120.get_b120_data(date_suffix='')
        self.assertEqual(result['info']['title'], 'B-120 Water Supply Forecast Summary (posted on 05/07/20 16:31)')
        self.assertEqual(result['info']['url'], 'https://cdec.water.ca.gov/b120.html')
        self.assertEqual(result['info']['type'], 'B120 Forecast')
        self.assertEqual(result['data']['Apr-Jul'].shape, (26, 6))
        self.assertEqual(result['data']['WY'].shape, (16, 14))

    def test_validate_date_suffix(self):
        """
        check the behavior of the validate_date_suffix method
        """
        self.assertTrue(b120.validate_date_suffix(''))
        self.assertTrue(b120.validate_date_suffix('_201804', min_year=2017))
        self.assertFalse(b120.validate_date_suffix('_201105', min_year=2017))

    def test_clean_td(self):
        """
        test to strip specified characters from text and convert to float or None, where applicable
        """
        self.assertEqual(b120.clean_td(' 8,000'), 8000)
        self.assertEqual(b120.clean_td('  5000 cfs'), '5000 cfs')
        self.assertIsNone(b120.clean_td(''))

    def test_get_b120_update_data(self):
        """
        test for B120 data-retrieval function relying on https://cdec.water.ca.gov/b120up.html
        """
        result = b120.get_b120_update_data(date_suffix='')
        self.assertEqual(result['info']['title'], 'B-120 Water Supply Forecast Update Summary (posted on 06/10/20 13:44)')
        self.assertEqual(result['info']['url'], 'https://cdec.water.ca.gov/b120up.html')
        self.assertEqual(result['info']['type'], 'B120 Update')
        self.assertEqual(result['data'].shape, (42, 9))

    def test_get_120_archived_reports(self):
        result = b120.get_120_archived_reports(2011, 4)
        self.assertEqual(result['info']['title'], '.T WRB120.201104 1104081414/')
        self.assertEqual(result['info']['url'], 'https://cdec.water.ca.gov/reportapp/javareports?name=B120.201104')
        self.assertEqual(result['info']['type'], 'B120 Forecast')
        self.assertEqual(result['info']['units'], 'TAF')

        self.assertEqual(result['data']['Apr-Jul'].shape, (26, 6))
        self.assertEqual(result['data']['Apr-Jul'].columns.tolist(), ['Hydrologic Region',
                                                                      'Watershed',
                                                                      'Apr-Jul Forecast',
                                                                      '% of Avg',
                                                                      '90% Exceedance',
                                                                      '10% Exceedance'])
        self.assertEqual(result['data']['WY'].shape, (16, 14))
        self.assertTrue('90% Exceedance' in result['data']['WY'].columns)

    def test_april_july_dataframe(self):
        data_list = [['SACRAMENTO RIVER', 'Sacramento River above Shasta Lake', 120.0, '41%', None, None],
                     ['SACRAMENTO RIVER', 'McCloud River above Shasta Lake', 260.0, '68%', None, None],
                     ['SACRAMENTO RIVER', 'Pit River above Shasta Lake', 680.0, '67%', None, None],
                     ['SACRAMENTO RIVER', 'Total Inflow to Shasta Lake', 1050.0, '60%', 860.0, 1210.0],
                     ['SACRAMENTO RIVER', 'Sacramento River above Bend Bridge', 1480.0, '61%', 1230.0, 1750.0],
                     ['SACRAMENTO RIVER', 'Feather River at Oroville', 940.0, '55%', 780.0, 1080.0],
                     ['SACRAMENTO RIVER', 'Yuba River near Smartsville', 600.0, '62%', 480.0, 710.0],
                     ['SACRAMENTO RIVER', 'American River below Folsom Lake', 790.0, '66%', 650.0, 950.0]]
        result = b120.april_july_dataframe(data_list)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.shape, (8, 6))
        self.assertEqual(result.columns.tolist(), ['Hydrologic Region', 'Watershed', 'Apr-Jul Forecast',
                                                   '% of Avg', '90% Exceedance', '10% Exceedance'])

#     def deferred_test_get_station_url(self):
#         result = cdec.get_station_url(station, start, end, data_format='CSV', sensors=[], duration='')
#         print(result)

#     def deferred_test_get_station_sensors(self):
#         result = cdec.get_station_sensors(station, start, end)
#         print(result)

#     def deferred_test_get_station_data(self):
#         result = cdec.get_station_data(station, start, end, sensors=[], duration='')
#         print(result)

#     def deferred_test_get_raw_station_csv(self):
#         result = cdec.get_raw_station_csv(station, start, end, sensors=[], duration='', filename='')
#         print(result)

#     def deferred_test_get_raw_station_json(self):
#         result = cdec.get_raw_station_json(station, start, end, sensors=[], duration='', filename='')
#         print(result)

#     def deferred_test_get_sensor_frame(self):
#         result = cdec.get_sensor_frame(station, start, end, sensor='', duration='')
#         print(result)

#     def deferred_test_get_station_metadata(self):
#         result = cdec.get_station_metadata(station, as_geojson=False)
#         print(result)

#     def deferred_test_get_dam_metadata(self):
#         result = cdec.get_dam_metadata(station)
#         print(result)

#     def deferred_test_get_reservoir_metadata(self):
#         result = cdec.get_reservoir_metadata(station)
#         print(result)

#     def deferred_test__get_table_index(self):
#         result = cdec._get_table_index(table_type, tables)
#         print(result)

#     def deferred_test__parse_station_generic_table(self):
#         result = cdec._parse_station_generic_table(table)
#         print(result)

#     def deferred_test__parse_station_sensors_table(self):
#         cdec._parse_station_sensors_table(table)

#     def deferred_test__parse_station_comments_table(self):
#         cdec._parse_station_comments_table(table)

#     def deferred_test__parse_data_available(self):
#         cdec._parse_data_available(text)

#     def deferred_test_get_data(self):
#         cdec.get_data(station, start, end, sensor='', duration='')

#     def deferred_test_get_daily_snowpack_data(self):
#         cdec.get_daily_snowpack_data(region, start, end)


# class TestSWP(unittest.TestCase):

    # def deferred_test_prompt_installation_and_exit(self):
    #     """
    #     test to ensure appropriate warning is printed when pdftotext is not installed; not yet implemented
    #     """
    #     swp.prompt_installation_and_exit()

    # def test_get_report_catalog(self):
    #     """
    #     test the default message behavior for get_report_catalog
    #     """ 
    #     result = swp.get_report_catalog(console=False)
    #     self.assertTrue('Oroville Operations' in result)
    #     self.assertTrue('Weekly Summaries' in result)

    # def test_get_report_url(self):
    #     """
    #     verify get_report_url produces the expected URL formats
    #     """
    #     # check one of the reservoir PDF reports
    #     expected_url = '/'.join(['https://water.ca.gov/-/media',
    #                              'DWR-Website',
    #                              'Web-Pages',
    #                              'Programs',
    #                              'State-Water-Project',
    #                              'Operations-And-Maintenance',
    #                              'Files',
    #                              'Operations-Control-Office',
    #                              'Project-Wide-Operations',
    #                              'Oroville-Weekly-Reservoir-Storage-Chart.pdf'])
    #     self.assertEqual(swp.get_report_url('Oroville'), expected_url)

    #     # check one of the txt-formatted reports
    #     expected_url = '/'.join(['https://data.cnra.ca.gov/dataset',
    #                              '742110dc-0d96-40bc-8e4e-f3594c6c4fe4',
    #                              'resource',
    #                              '45c01d10-4da2-4ebb-8927-367b3bb1e601',
    #                              'download',
    #                              'dispatchers-monday-water-report.txt'])
    #     self.assertEqual(swp.get_report_url('Mon'), expected_url)

    #     # check for invalid input
    #     self.assertIsNone(swp.get_report_url('invalid'))

    # def test_get_raw_text(self):
    #     """
    #     test expected behavior for get_raw_text for pdf report and invalid text report
    #     """
    #     # test for a PDF-formatted report
    #     result = swp.get_raw_text('Delta Operations Summary (daily)')
    #     self.assertIsInstance(result, str)
    #     self.assertTrue(result.startswith('PRELIMINARY DATA'))
    #     self.assertTrue(result.strip().endswith('please contact OCO_Export_Management@water.ca.gov'))

    #     # test for a text-formatted report
    #     self.assertRaises(ValueError, swp.get_raw_text, 'Mon')

    # def test_get_delta_daily_data(self):
    #     result = swp.get_delta_daily_data('dict')
    #     self.assertTrue(result['info']['title'].startswith('EXECUTIVE OPERATIONS SUMMARY ON '))
    #     self.assertIsInstance(result['data'], dict)
    #     self.assertTrue('Reservoir Releases' in result['data'])

    # def test_get_barker_slough_data(self):
    #     result = swp.get_barker_slough_data()
    #     self.assertEqual(result['info']['title'], 'BARKER SLOUGH PUMPING PLANT WEEKLY REPORT')
    #     self.assertEqual(result['data'].shape, (7, 3))
    #     self.assertIsInstance(result['data'].index, pd.core.indexes.datetimes.DatetimeIndex)

    # def test_get_oco_tabular_data(self):
    #     """
    #     test tabular data extraction for the Water Quality Summary report using get_oco_tabular_data
    #     """
    #     result = swp.get_oco_tabular_data('Water Quality Summary (daily)')
    #     self.assertEqual(result['info']['filename'], 'Delta-Water-Quality-Daily-Summary.pdf')
    #     self.assertIsInstance(result['info']['pages'], int)
    #     self.assertIsInstance(result['data'], pd.DataFrame)
    #     self.assertEqual(result['data'].shape, (30, 46))
    #     self.assertEqual(result['data'].index.name, 'Date (30 days)')


# class TestWSI(unittest.TestCase):
#     pass


if __name__ == '__main__':
    unittest.main()
