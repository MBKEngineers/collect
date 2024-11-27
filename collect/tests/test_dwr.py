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

from bs4 import BeautifulSoup
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

    def test_get_station_url(self):
        """
        test the creation of query URL for a particular station, sensor set, data interval, and date range
        """
        result = cdec.get_station_url('CFW',
                                      dt.datetime(2023, 1, 1),
                                      dt.datetime(2023, 1, 3),
                                      data_format='CSV',
                                      sensors=[6], 
                                      duration='H')
        self.assertEqual(result, '&'.join(['https://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet?Stations=CFW',
                                           'dur_code=H',
                                           'SensorNums=6',
                                           'Start=2023-01-01',
                                           'End=2023-01-03']))

    def test_get_data(self):
        """
        test retrieval of station timeseries and details data
        """
        result = cdec.get_data('CFW', dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 3), sensor=6, duration='D')
        self.assertIsInstance(result, dict)
        self.assertIsInstance(result['info'], dict)
        self.assertEqual(result['info']['title'], 'BEAR RIVER AT CAMP FAR WEST DAM')
        self.assertIsInstance(result['data'], pd.DataFrame)
        self.assertEqual(result['data']['VALUE'].values.tolist(), [300.48, 300.98, 300.72])

    def test_get_sensor_frame(self):
        """
        test timeseries data retrieval using the CSV query service for a particular date range and sensor combo
        """
        result = cdec.get_sensor_frame('CFW', dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 3), sensor=15, duration='D')
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result['VALUE'].values.tolist(), [105419.0, 106489.0, 105931.0])

    def test_get_station_data(self):
        """
        test duplicate function (with get_raw_station_csv) for retrieval of timeseries data
        """
        result = cdec.get_station_data('CFW',
                                       dt.datetime(2023, 1, 1),
                                       dt.datetime(2023, 6, 3),
                                       sensors=[15],
                                       duration='M')
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.shape, (6, 9))
        self.assertEqual(result.tail(1).values.tolist()[0][:6], ['CFW', 'M', 15, 'STORAGE', '20230601 0000', 73931.0])

    def test_get_raw_station_csv(self):
        """
        test expected values for an hourly elevation data query
        """
        result = cdec.get_raw_station_csv('CFW',
                                          dt.datetime(2023, 1, 1),
                                          dt.datetime(2023, 1, 3),
                                          sensors=[6],
                                          duration='H',
                                          filename='')
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.shape, (49, 9))
        self.assertEqual(result.tail(1).values.tolist()[0][:6], ['CFW', 'H', 6, 'RES ELE', '20230103 0000', 300.98])

    def test_get_raw_station_json(self):
        """
        test retrieval of timeseries station data using the JSON query service
        """
        result = cdec.get_raw_station_json('CFW',
                                           dt.datetime(2023, 1, 1),
                                           dt.datetime(2023, 1, 4),
                                           sensors=[15],
                                           duration='D',
                                           filename='')
        self.assertIsInstance(result, list)
        self.assertEqual([(x['date'], x['value']) for x in result], [('2023-1-1 00:00', 105419),
                                                                     ('2023-1-2 00:00', 106489),
                                                                     ('2023-1-3 00:00', 105931),
                                                                     ('2023-1-4 00:00', 105185)])

    def test_get_station_metadata(self):
        """
        test for retrieving station information from the CDEC detail page
        """
        result = cdec.get_station_metadata('CFW', as_geojson=False)
        self.assertIsInstance(result, dict)
        self.assertIsInstance(result['info'], dict)
        self.assertIsInstance(result['info']['dam'], dict)
        self.assertIsInstance(result['info']['reservoir'], dict)
        self.assertIsInstance(result['info']['sensors'], dict)
        self.assertEqual(result['info']['title'], 'BEAR RIVER AT CAMP FAR WEST DAM')
        self.assertEqual(result['info']['Station ID'], 'CFW')
        self.assertEqual(result['info']['Latitude'], '39.049858°')

    def test_get_dam_metadata(self):
        """
        test for retrieving dam information from the CDEC detail page
        """
        result = cdec.get_dam_metadata('CFW')
        self.assertIsInstance(result, dict)
        self.assertIsInstance(result['dam'], dict)
        self.assertEqual(result['dam']['title'], 'Dam Information')
        self.assertEqual(result['dam']['Station ID'], 'CFW')
        self.assertEqual(result['dam']['Dam Name'], 'CAMP FAR WEST')
        self.assertEqual(result['dam']['National ID'], 'CA00227')

    def test_get_reservoir_metadata(self):
        """
        test for retrieving reservoir information from the CDEC detail page
        """
        result = cdec.get_reservoir_metadata('CFW')
        self.assertIsInstance(result, dict)
        self.assertIsInstance(result['reservoir'], dict)
        self.assertEqual(result['reservoir']['title'], 'BEAR RIVER AT CAMP FAR WEST DAM (CFW)')
        self.assertEqual(result['reservoir']['Station ID'], 'CFW')
        self.assertEqual(result['reservoir']['Stream Name'], 'Bear River')
        self.assertEqual(result['reservoir']['Capacity'], '104,500 af')

    def test__get_table_index(self):
        """
        test function used to determine position of table in station detail page relative to other tables
        """
        self.assertEqual(cdec.queries._get_table_index('site', [1]), 0)
        self.assertEqual(cdec.queries._get_table_index('datum', [1, 1, 1, 1]), 1)
        self.assertIsNone(cdec.queries._get_table_index('datum', [1, 1, 1]))
        self.assertEqual(cdec.queries._get_table_index('sensors', [1, 1, 1, 1]), 2)
        self.assertEqual(cdec.queries._get_table_index('comments', [1, 1, 1, 1]), 3)
        self.assertIsNone(cdec.queries._get_table_index('other', []))

    def test__parse_station_generic_table(self):
        """
        test extraction of station general information and data availability from station detail page
        """
        table = BeautifulSoup(textwrap.dedent("""\
            <table border="1">
                <tr>
                    <td><b>Station ID</b></td>
                    <td>CFW</td>
                    <td><b>Elevation</b></td>
                    <td>260 ft</td>
                </tr>
                <tr>
                    <td><b>River Basin</b></td>
                    <td>BEAR RIVER</td>
                    <td><b>County</b></td>
                    <td>YUBA</td>
                </tr>
                <tr>
                    <td><b>Hydrologic Area</b></td>
                    <td>SACRAMENTO RIVER</td>
                    <td><b>Nearby City</b></td>
                    <td>MARYSVILLE</td>
                </tr>
                <tr>
                    <td><b>Latitude</b></td>
                    <td>39.049858°</td>
                    <td><b>Longitude</b></td>
                    <td>-121.315941°</td>
                </tr>
                <tr>
                    <td><b>Operator</b></td>
                    <td>CA Dept of Water Resources/DFM-Hydro-SMN</td>
                    <td><b>Maintenance</b></td>
                    <td>CA Dept of Water Resources/DFM-Hydro-SMN</td>
                </tr>
            </table>
        """), 'lxml')
        result = cdec.queries._parse_station_generic_table(table)
        self.assertEqual(result, {'Station ID': 'CFW',
                                  'Elevation': '260 ft',
                                  'River Basin': 'BEAR RIVER',
                                  'County': 'YUBA',
                                  'Hydrologic Area': 'SACRAMENTO RIVER',
                                  'Nearby City': 'MARYSVILLE',
                                  'Latitude': '39.049858°',
                                  'Longitude': '-121.315941°',
                                  'Operator': 'CA Dept of Water Resources/DFM-Hydro-SMN',
                                  'Maintenance': 'CA Dept of Water Resources/DFM-Hydro-SMN'})

    def test__parse_station_sensors_table(self):
        """
        test extraction of sensor information and data availability from station detail page
        """
        table = BeautifulSoup(textwrap.dedent("""\
            <table border="0" width="800">
                <th bgcolor="e0e0e0"><b>Sensor Description</b></th>
                <th bgcolor="e0e0e0"><b>Sensor Number</b></th>
                <th bgcolor="e0e0e0"><b>Duration</b></th>
                <th bgcolor="e0e0e0"><b>Plot</b></th>
                <th bgcolor="e0e0e0"><b>Data Collection</b></th>
                <th bgcolor="e0e0e0"><b>Data Available</b></th><p></p>
                <tr>
                    <td align="left"><b>FLOW, RIVER DISCHARGE</b>, CFS</td><p></p>
                    <td align="center"><b>20</b></td>
                    <td width="120"> (<a href="/dynamicapp/QueryF?s=CFW">event</a>) </td>
                    <td width="120">(<a href="/jspplot/jspPlotServlet.jsp?sensor_no=7581&amp;end=&amp;geom=small&amp;interval=2&amp;cookies=cdec01">FLOW</a>)</td>
                    <td align="left" width="180">COMPUTED</td>
                    <td align="center" width="180"> 01/01/2021 to 01/01/2023</td>
                </tr>
            </table>
        """), 'lxml')
        result = cdec.queries._parse_station_sensors_table(table)
        self.assertEqual(result, {'20': {'event': {'description': 'FLOW, RIVER DISCHARGE, CFS',
                                                   'sensor': '20',
                                                   'duration': 'event',
                                                   'collection': 'COMPUTED',
                                                   'availability': '01/01/2021 to 01/01/2023',
                                                   'years': [2021, 2022, 2023]}}})

    def test__parse_station_comments_table(self):
        table = BeautifulSoup(textwrap.dedent("""\
            <table border="0" width="800">
                <tr>
                    <td width="100"><b>02/28/2023</b></td>
                    <td>Example comment about data availability.</td>
                </tr>
                <tr><td width="100"><b>04/27/2020</b></td>
                    <td>Example comment about datum info.</td>
                </tr>
            </table>
        """), 'lxml')
        result = cdec.queries._parse_station_comments_table(table)
        self.assertEqual(result, {'02/28/2023': 'Example comment about data availability.',
                                  '04/27/2020': 'Example comment about datum info.'})

    def test__parse_data_available(self):
        """
        test generation of year list for the data availability from sensor table on station detail page
        """
        result = cdec.queries._parse_data_available('01/01/2021 to 01/01/2023')
        self.assertEqual(result, [2021, 2022, 2023])

    def test_get_daily_snowpack_data(self):
        """
        test for retrieving past daily snowpack data
        """
        result = cdec.get_daily_snowpack_data('CENTRAL', dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 3))
        self.assertEqual(result['info']['interval'], 'daily')
        self.assertEqual(result['info']['region'], 'CENTRAL')
        self.assertEqual(result['data'].shape, (3, 5))
        self.assertEqual(result['data'].tail(1).values.tolist(), [['CENTRAL', 53, 19.0, 70, 185]])

try:
    import pdftotext

    class TestSWP(unittest.TestCase):

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

except:
    print('Module pdftotext is required for collect.dwr.swp testing.  Install with `pip install pdftotext==2.2.2`')


class TestWSI(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
