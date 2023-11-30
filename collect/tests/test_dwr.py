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


class TestB120(unittest.TestCase):
    pass


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
        cawdl.get_cawdl_data('17202')

    def deferred_test_get_cawdl_surface_water_data(self):
        cawdl.get_cawdl_surface_water_data('17202', 2021, 'FLOW', interval='DAILY_MEAN')

    def deferred_test_get_cawdl_surface_water_por(self):
        cawdl.get_cawdl_surface_water_por('17202', 'FLOW', interval='DAILY_MEAN')

    def deferred_test_get_cawdl_surface_water_site_report(self):
        cawdl.get_cawdl_surface_water_site_report('17202')


class TestCDEC(unittest.TestCase):

    def deferred_test_get_b120_data(self):
        b120.get_b120_data(date_suffix='')

    def deferred_test_validate_date_suffix(self):
        b120.validate_date_suffix(date_suffix, min_year=2017)

    def deferred_test_clean_td(self):
        b120.clean_td(text)

    def deferred_test_get_b120_update_data(self):
        b120.get_b120_update_data(date_suffix='')

    def deferred_test_get_120_archived_reports(self):
        b120.get_120_archived_reports(year, month)

    def deferred_test_april_july_dataframe(self):
        b120.april_july_dataframe(data_list)

    def deferred_test_get_station_url(self):
        cdec.get_station_url(station, start, end, data_format='CSV', sensors=[], duration='')

    def deferred_test_get_station_sensors(self):
        cdec.get_station_sensors(station, start, end)

    def deferred_test_get_station_data(self):
        cdec.get_station_data(station, start, end, sensors=[], duration='')

    def deferred_test_get_raw_station_csv(self):
        cdec.get_raw_station_csv(station, start, end, sensors=[], duration='', filename='')

    def deferred_test_get_raw_station_json(self):
        cdec.get_raw_station_json(station, start, end, sensors=[], duration='', filename='')

    def deferred_test_get_sensor_frame(self):
        cdec.get_sensor_frame(station, start, end, sensor='', duration='')

    def deferred_test_get_station_metadata(self):
        cdec.get_station_metadata(station, as_geojson=False)

    def deferred_test_get_dam_metadata(self):
        cdec.get_dam_metadata(station)

    def deferred_test_get_reservoir_metadata(self):
        cdec.get_reservoir_metadata(station)

    def deferred_test__get_table_index(self):
        cdec._get_table_index(table_type, tables)

    def deferred_test__parse_station_generic_table(self):
        cdec._parse_station_generic_table(table)

    def deferred_test__parse_station_sensors_table(self):
        cdec._parse_station_sensors_table(table)

    def deferred_test__parse_station_comments_table(self):
        cdec._parse_station_comments_table(table)

    def deferred_test__parse_data_available(self):
        cdec._parse_data_available(text)

    def deferred_test_get_data(self):
        cdec.get_data(station, start, end, sensor='', duration='')

    def deferred_test_get_daily_snowpack_data(self):
        cdec.get_daily_snowpack_data(region, start, end)


class TestSWP(unittest.TestCase):

    def deferred_test_prompt_installation_and_exit(self):
        """
        test to ensure appropriate warning is printed when pdftotext is not installed; not yet implemented
        """
        swp.prompt_installation_and_exit()

    def test_get_report_catalog(self):
        """
        test the default message behavior for get_report_catalog
        """ 
        result = swp.get_report_catalog(console=False)
        self.assertTrue('Oroville Operations' in result)
        self.assertTrue('Weekly Summaries' in result)

    def test_get_report_url(self):
        """
        verify get_report_url produces the expected URL formats
        """
        # check one of the reservoir PDF reports
        expected_url = '/'.join(['https://water.ca.gov/-/media',
                                 'DWR-Website',
                                 'Web-Pages',
                                 'Programs',
                                 'State-Water-Project',
                                 'Operations-And-Maintenance',
                                 'Files',
                                 'Operations-Control-Office',
                                 'Project-Wide-Operations',
                                 'Oroville-Weekly-Reservoir-Storage-Chart.pdf'])
        self.assertEqual(swp.get_report_url('Oroville'), expected_url)

        # check one of the txt-formatted reports
        expected_url = '/'.join(['https://data.cnra.ca.gov/dataset',
                                 '742110dc-0d96-40bc-8e4e-f3594c6c4fe4',
                                 'resource',
                                 '45c01d10-4da2-4ebb-8927-367b3bb1e601',
                                 'download',
                                 'dispatchers-monday-water-report.txt'])
        self.assertEqual(swp.get_report_url('Mon'), expected_url)

        # check for invalid input
        self.assertIsNone(swp.get_report_url('invalid'))

    def test_get_raw_text(self):
        """
        test expected behavior for get_raw_text for pdf report and invalid text report
        """
        # test for a PDF-formatted report
        result = swp.get_raw_text('Delta Operations Summary (daily)')
        self.assertIsInstance(result, str)
        self.assertTrue(result.startswith('PRELIMINARY DATA'))
        self.assertTrue(result.strip().endswith('please contact OCO_Export_Management@water.ca.gov'))

        # test for a text-formatted report
        self.assertRaises(ValueError, swp.get_raw_text, 'Mon')

    def test_get_delta_daily_data(self):
        result = swp.get_delta_daily_data('dict')
        self.assertTrue(result['info']['title'].startswith('EXECUTIVE OPERATIONS SUMMARY ON '))
        self.assertIsInstance(result['data'], dict)
        self.assertTrue('Reservoir Releases' in result['data'])

    def test_get_barker_slough_data(self):
        result = swp.get_barker_slough_data()
        self.assertEqual(result['info']['title'], 'BARKER SLOUGH PUMPING PLANT WEEKLY REPORT')
        self.assertEqual(result['data'].shape, (7, 3))
        self.assertIsInstance(result['data'].index, pd.core.indexes.datetimes.DatetimeIndex)

    def test_get_oco_tabular_data(self):
        """
        test tabular data extraction for the Water Quality Summary report using get_oco_tabular_data
        """
        result = swp.get_oco_tabular_data('Water Quality Summary (daily)')
        self.assertEqual(result['info']['filename'], 'Delta-Water-Quality-Daily-Summary.pdf')
        self.assertIsInstance(result['info']['pages'], int)
        self.assertIsInstance(result['data'], pd.DataFrame)
        self.assertEqual(result['data'].shape, (30, 46))
        self.assertEqual(result['data'].index.name, 'Date (30 days)')


class TestWSI(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
