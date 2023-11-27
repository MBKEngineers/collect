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

from dotenv import load_dotenv
import pandas as pd

from collect.dwr import cdec
from collect.dwr import casgem
from collect.dwr import cawdl
from collect.dwr import b120
from collect.dwr import swp

from collect import alert
from collect import cnrfc
from collect import cvo
from collect import nid
from collect import usgs
from collect import utils
from collect.usace import wcds


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
        self.assertEqual(alert.get_sites_from_list(as_dataframe=True, sensor_class=None).shape, (128, 4))

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


class TestCNRFC(unittest.TestCase):

    @property
    def deterministic_frame(self):
        """
        fixture for testing watershed deterministic file handling
        """
        if not hasattr(self, '_deterministic_frame'):
            text_data = io.StringIO(textwrap.dedent("""\
                GMT,CMPC1,NHGC1,MSGC1,FRGC1,EDOC1,SOSC1,MHBC1,MCNC1
                ,QINE,QINE,QINE,QINE,QINE,QINE,QINE,QINE
                2019-03-30 12:00:00,2.45972,0.70641,0.08901,0.22803,1.03512,0.71908,2.83132,2.58248
                2019-03-30 13:00:00,2.44774,0.67366,0.08901,0.21302,1.03512,0.70908,2.88032,2.56875
                2019-03-30 14:00:00,2.43568,0.67408,0.08901,0.19602,1.03011,0.71208,2.84732,2.53694
                2019-03-30 15:00:00,2.42353,0.67424,0.08901,0.22903,1.02611,0.70608,2.83132,2.52791
                2019-03-30 16:00:00,2.41129,0.67558,0.08901,0.20202,1.02211,0.70208,2.83132,2.50098
                2019-03-30 17:00:00,2.39895,0.60832,0.08901,0.21002,1.01811,0.70208,2.81431,2.4876
                2019-03-30 18:00:00,2.38652,0.64266,0.08901,0.18302,1.00911,0.69608,2.83132,2.46544
                2019-03-30 19:00:00,2.38077,0.67591,0.08701,0.20202,1.00511,0.69208,2.79831,2.45222
                2019-03-30 20:00:00,2.37473,0.67491,0.08701,0.18602,1.00111,0.69208,2.79831,2.44343
                2019-03-30 21:00:00,2.36843,0.67599,0.08601,0.19602,0.99211,0.68908,2.79831,2.42595
                2019-03-30 22:00:00,2.36185,0.67599,0.08601,0.03374,0.99211,0.68208,2.74931,2.41724
                2019-03-30 23:00:00,2.35498,0.71033,0.08601,0.19102,0.98411,0.68208,2.78231,2.40856
                2019-03-31 00:00:00,2.34785,0.67608,0.08401,0.16702,0.98011,0.67608,2.74931,2.39559
                2019-03-31 01:00:00,2.32832,0.67508,0.08401,0.19902,0.97111,0.66607,2.7163,2.38698
                2019-03-31 02:00:00,2.30886,0.67608,0.08401,0.16302,0.96311,0.65907,2.7003,2.36982
                2019-03-31 03:00:00,2.28949,0.64274,0.08401,0.19302,0.96311,0.65607,2.7163,2.36555
                2019-03-31 04:00:00,2.2702,0.6084,0.08401,0.03239,0.95511,0.66907,2.7163,2.34852
                2019-03-31 05:00:00,2.25098,0.60724,0.08401,0.17702,0.94711,0.65907,2.6843,2.34004
                2019-03-31 06:00:00,2.23185,0.64141,0.08401,0.15302,0.9261,0.65907,2.6683,2.33159
                2019-03-31 07:00:00,2.22434,0.60915,0.08401,0.16402,0.9141,0.65607,2.6843,2.31896
                2019-03-31 08:00:00,2.21675,0.5749,0.08201,0.17202,0.9141,0.66207,2.62029,2.3022
                2019-03-31 09:00:00,2.2091,0.60815,0.08201,0.15802,0.9101,0.65907,2.63629,2.2897
                2019-03-31 10:00:00,2.20137,0.64241,0.08101,0.16702,0.9141,0.65907,2.58829,2.27725
                2019-03-31 11:00:00,2.19357,0.60924,0.08101,0.16802,0.9141,0.65907,2.57229,2.26486
                2019-03-31 12:00:00,2.1857,0.57507,0.08101,0.15402,0.9101,0.65307,2.57229,2.25253
                2019-03-31 13:00:00,2.17421,0.60832,0.08101,0.15102,0.9141,0.65307,2.58829,2.23544
                2019-03-31 14:00:00,2.16274,0.64257,0.08101,0.18902,0.9101,0.65607,2.55728,2.21627
                2019-03-31 15:00:00,2.15131,0.60832,0.08101,0.03094,0.9101,0.64907,2.57229,2.20199
                2019-03-31 16:00:00,2.1399,0.54081,0.08101,0.14802,0.9061,0.64307,2.55728,2.18779
                2019-03-31 17:00:00,2.12853,0.54081,0.08101,0.03072,0.9061,0.64607,2.57229,2.16429
                2019-03-31 18:00:00,2.11718,0.57515,0.08101,0.14502,0.8981,0.64607,2.57229,2.15495
                2019-03-31 19:00:00,2.11344,0.57523,0.08101,0.15802,0.9021,0.64007,2.55728,2.13637
                2019-03-31 20:00:00,2.10957,0.57531,0.07901,0.14302,0.8981,0.64307,2.54128,2.13174
                2019-03-31 21:00:00,2.10557,0.5764,0.07901,0.16502,0.8861,0.63707,2.55728,2.12713
                2019-03-31 22:00:00,2.10143,0.63047,0.07901,0.15202,0.8901,0.62707,2.54128,2.11793
                2019-03-31 23:00:00,2.09715,0.6617,0.07901,0.13502,0.8821,0.62707,2.54128,2.11793
                2019-04-01 00:00:00,2.09274,0.64507,0.07901,0.03001,0.8781,0.61807,2.51028,2.11334
                2019-04-01 01:00:00,2.08882,0.61182,0.07701,0.02992,0.8741,0.62107,2.52628,2.10875
                2019-04-01 02:00:00,2.08483,0.51206,0.07701,0.02983,0.8701,0.61807,2.49528,2.09962
                2019-04-01 03:00:00,2.08079,0.51205,0.07701,0.02974,0.8661,0.61207,2.48028,2.09506
                2019-04-01 04:00:00,2.07668,0.51206,0.07701,0.02964,0.8621,0.61207,2.49528,2.09051
                2019-04-01 05:00:00,2.07251,0.51206,0.07701,0.02955,0.8541,0.61507,2.48028,2.08144
                2019-04-01 06:00:00,2.06829,0.51206,0.07701,0.02946,0.85109,0.62107,2.44927,2.07692
                2019-04-01 07:00:00,2.07789,0.51206,0.07701,0.13001,0.84709,0.62407,2.43427,2.0679
                2019-04-01 08:00:00,2.08712,0.51206,0.07701,0.02929,0.84709,0.63007,2.44927,2.0634
                2019-04-01 09:00:00,2.09597,0.51206,0.07701,0.13502,0.84709,0.62107,2.41927,2.04996
                2019-04-01 10:00:00,2.10444,0.50556,0.07701,0.02911,0.84709,0.63407,2.43427,2.04104
                2019-04-01 11:00:00,2.11255,0.60507,0.07601,0.02903,0.84709,0.63407,2.41927,2.02772
                2019-04-01 12:00:00,2.12029,0.63774,0.07601,0.02894,0.84709,0.62707,2.41927,2.01888
                2019-04-01 13:00:00,2.12346,0.59182,0.07601,0.11601,0.85109,0.63707,2.38927,2.00568
                2019-04-01 14:00:00,2.12662,0.55896,0.07601,0.11201,0.85109,0.63407,2.41927,1.99255
                2019-04-01 15:00:00,2.1298,0.57073,0.07401,0.12301,0.85109,0.62707,2.40427,1.98384
                2019-04-01 16:00:00,2.13297,0.5924,0.07401,0.12401,0.85109,0.63007,2.43427,1.97516
                2019-04-01 17:00:00,2.13613,0.54539,0.07401,0.12901,0.84709,0.62707,2.41927,1.96652
                2019-04-01 18:00:00,2.13929,0.53298,0.07401,0.12101,0.85109,0.63007,2.25725,1.95791
                2019-04-01 19:00:00,2.14021,0.56206,0.07301,0.10801,0.84309,0.62107,2.25725,1.95791
                2019-04-01 20:00:00,2.14111,0.56231,0.07301,0.12001,0.84309,0.62107,2.27225,1.95361
                2019-04-01 21:00:00,2.142,0.52906,0.07301,0.10601,0.83909,0.61807,2.27225,1.94932"""))
            self._deterministic_frame = pd.read_csv(text_data,
                                                    header=0,
                                                    skiprows=[1,],
                                                    nrows=60,
                                                    parse_dates=True,
                                                    index_col=0,
                                                    float_precision='high',
                                                    dtype={'GMT': str}).mul(1000)
        return self._deterministic_frame
    
    def test_cnrfc_credentials(self):
        """
        load sensitive info from .env file and test CNRFC credentials exist
        """
        load_dotenv()
        self.assertTrue(('CNRFC_USER' in os.environ) & ('CNRFC_PASSWORD' in os.environ))

    def test_convert_date_columns(self):
        """Ensure datetime data converted to string format"""
        test_index = self.deterministic_frame.index.strftime('%Y-%m-%d')
        self.assertEqual(test_index.tolist()[0], '2019-03-30')

    def test_validate_duration(self):
        """
        function to properly format/case hourly or daily durations
        """
        duration = 'Hourly'
        self.assertEqual(cnrfc.cnrfc._validate_duration(duration), 'hourly')
            
    def test_validate_duration_invalid(self):
        """
        test that invalid duration raises a ValueError
        """
        bad_input = 'monthly'
        self.assertRaises(ValueError,
                          cnrfc.cnrfc._validate_duration,
                          bad_input)

    def test_get_deterministic_forecast(self):
        """
        Test that deterministic forecast start from Graphical_RVF page matches
        CSV start of forecast
        """
        cnrfc_id = 'FOLC1'
        first_ordinate = cnrfc.get_forecast_meta_deterministic(cnrfc_id, first_ordinate=True)[-1]
        df = cnrfc.get_deterministic_forecast(cnrfc_id, truncate_historical=False)['data']
        first_forecast_entry = df['forecast'].dropna().index.tolist()[0]

        # check that the date/time representation in the timestamp and datetime.datetime objects are the same
        self.assertEqual(first_forecast_entry.year, first_ordinate.year)
        self.assertEqual(first_forecast_entry.month, first_ordinate.month)
        self.assertEqual(first_forecast_entry.day, first_ordinate.day)
        self.assertEqual(first_forecast_entry.hour, first_ordinate.hour)
        self.assertEqual(first_forecast_entry.minute, first_ordinate.minute)

        # for now, strip the local tzinfo from `first_ordinate`
        self.assertEqual(first_forecast_entry.tzinfo, first_ordinate.replace(tzinfo=None).tzinfo)

    def test_get_deterministic_forecast_watershed(self):
        """
        test watershed deterministic forecast download for North San Joaquin on a particular date
        """
        df = cnrfc.get_deterministic_forecast_watershed('N_SanJoaquin', '2019040412')['data']
        self.assertEqual(df.head(20)['NHGC1'].values.tolist(),
                         self.deterministic_frame.head(20)['NHGC1'].values.tolist())

    def test_get_water_year_trend_tabular(self):
        """
        test watershed deterministic forecast download for North San Joaquin on a 
        particular date
        """
        df = cnrfc.get_water_year_trend_tabular('FOLC1', '2022')['data']
        self.assertEqual(df.shape, (365, 9))

    # def test_get_seasonal_trend_tabular(self):
    #     cnrfc.get_seasonal_trend_tabular(cnrfc_id, water_year)

    # def test_get_water_year_trend_tabular(self):
    #     cnrfc.get_water_year_trend_tabular(cnrfc_id, water_year)

    # def test_get_deterministic_forecast(self):
    #     cnrfc.get_deterministic_forecast(cnrfc_id, truncate_historical=False, release=False)

    # def test_get_deterministic_forecast_watershed(self):
    #     cnrfc.get_deterministic_forecast_watershed(watershed,
    #                                                date_string,
    #                                                acre_feet=False,
    #                                                pdt_convert=False,
    #                                                as_pdt=False,
    #                                                cnrfc_id=None)

    # def test_get_forecast_meta_deterministic(self):
    #     cnrfc.get_forecast_meta_deterministic(cnrfc_id, first_ordinate=False, release=False)

    # def test_get_ensemble_forecast(self):
    #     cnrfc.get_ensemble_forecast(cnrfc_id, duration, acre_feet=False, pdt_convert=False, as_pdt=False)

    # def test_get_ensemble_forecast_watershed(self):
    #     cnrfc.get_ensemble_forecast_watershed(watershed,
    #                                           duration,
    #                                           date_string,
    #                                           acre_feet=False,
    #                                           pdt_convert=False,
    #                                           as_pdt=False,
    #                                           cnrfc_id=None)

    # def test_download_watershed_file(self):
    #     cnrfc.download_watershed_file(watershed, date_string, forecast_type, duration=None, path=None)

    # def test_get_watershed_forecast_issue_time(self):
    #     cnrfc.get_watershed_forecast_issue_time(duration, watershed, date_string=None, deterministic=False)

    # def test_get_watershed(self):
    #     cnrfc.get_watershed(cnrfc_id)

    # def test_get_ensemble_first_forecast_ordinate(self):
    #     cnrfc.get_ensemble_first_forecast_ordinate(url=None, df=None)

    # def test_get_ensemble_product_url(self):
    #     cnrfc.get_ensemble_product_url(product_id, cnrfc_id, data_format='')

    # def test_get_ensemble_product_1(self):
    #     cnrfc.get_ensemble_product_1(cnrfc_id)

    # def test_get_ensemble_product_2(self):
    #     cnrfc.get_ensemble_product_2(cnrfc_id)

    # def test_get_ensemble_product_3(self):
    #     cnrfc.get_ensemble_product_3(cnrfc_id)

    # def test_get_ensemble_product_5(self):
    #     cnrfc.get_ensemble_product_5(cnrfc_id)

    # def test_get_ensemble_product_6(self):
    #     cnrfc.get_ensemble_product_6(cnrfc_id)

    # def test_get_ensemble_product_10(self):
    #     cnrfc.get_ensemble_product_10(cnrfc_id)

    # def test_get_ensemble_product_11(self):
    #     cnrfc.get_ensemble_product_11(cnrfc_id)

    # def test_get_ensemble_product_12(self):
    #     cnrfc.get_ensemble_product_12(cnrfc_id)

    # def test_get_ensemble_product_13(self):
    #     cnrfc.get_ensemble_product_13(cnrfc_id)

    # def test_get_data_report_part_8(self):
    #     cnrfc.get_data_report_part_8()

    # def test_get_monthly_reservoir_storage_summary(self):
    #     cnrfc.get_monthly_reservoir_storage_summary()

    # def test_esp_trace_analysis_wrapper(self):
    #     cnrfc.esp_trace_analysis_wrapper()

    # def test__apply_conversions(self):
    #     cnrfc._apply_conversions(df, duration, acre_feet, pdt_convert, as_pdt)

    # def test__get_cnrfc_restricted_content(self):
    #     cnrfc._get_cnrfc_restricted_content(url)

    # def test__get_forecast_csv(self):
    #     cnrfc._get_forecast_csv(url)

    # def test_get_forecast_csvdata(self):
    #     cnrfc.get_forecast_csvdata(url)

    # def test_get_rating_curve(self):
    #     cnrfc.get_rating_curve(cnrfc_id)

    # def test__default_date_string(self):
    #     cnrfc._default_date_string(date_string)

    # def test__parse_blue_table(self):
    #     cnrfc._parse_blue_table(table_soup)


# class TestCASGEM(unittest.TestCase):

    # def test_get_casgem_data(self):
    #     result = casgem.get_casgem_data(casgem_id=None,
    #                                     state_well_number=None,
    #                                     local_well_designation=None,
    #                                     master_site_code=None,
    #                                     write_to_html_file=False)
    #     print(result)


# class TestCAWDL(unittest.TestCase):

    # def test_get_cawdl_data(self):
    #     cawdl.get_cawdl_data(site_id)

    # def test_get_cawdl_surface_water_data(self):
    #     cawdl.get_cawdl_surface_water_data(site_id, water_year, variable, interval=None)

    # def test_get_cawdl_surface_water_por(self):
    #     cawdl.get_cawdl_surface_water_por(site_id, variable, interval=None)

    # def test_get_cawdl_surface_water_site_report(self):
    #     cawdl.get_cawdl_surface_water_site_report(site_id)


# class TestCDEC(unittest.TestCase):

    # def test_get_b120_data(self):
    #     b120.get_b120_data(date_suffix='')

    # def test_validate_date_suffix(self):
    #     b120.validate_date_suffix(date_suffix, min_year=2017)

    # def test_clean_td(self):
    #     b120.clean_td(text)

    # def test_get_b120_update_data(self):
    #     b120.get_b120_update_data(date_suffix='')

    # def test_get_120_archived_reports(self):
    #     b120.get_120_archived_reports(year, month)

    # def test_april_july_dataframe(self):
    #     b120.april_july_dataframe(data_list)

    # def test_get_station_url(self):
    #     cdec.get_station_url(station, start, end, data_format='CSV', sensors=[], duration='')

    # def test_get_station_sensors(self):
    #     cdec.get_station_sensors(station, start, end)

    # def test_get_station_data(self):
    #     cdec.get_station_data(station, start, end, sensors=[], duration='')

    # def test_get_raw_station_csv(self):
    #     cdec.get_raw_station_csv(station, start, end, sensors=[], duration='', filename='')

    # def test_get_raw_station_json(self):
    #     cdec.get_raw_station_json(station, start, end, sensors=[], duration='', filename='')

    # def test_get_sensor_frame(self):
    #     cdec.get_sensor_frame(station, start, end, sensor='', duration='')

    # def test_get_station_metadata(self):
    #     cdec.get_station_metadata(station, as_geojson=False)

    # def test_get_dam_metadata(self):
    #     cdec.get_dam_metadata(station)

    # def test_get_reservoir_metadata(self):
    #     cdec.get_reservoir_metadata(station)

    # def test__get_table_index(self):
    #     cdec._get_table_index(table_type, tables)

    # def test__parse_station_generic_table(self):
    #     cdec._parse_station_generic_table(table)

    # def test__parse_station_sensors_table(self):
    #     cdec._parse_station_sensors_table(table)

    # def test__parse_station_comments_table(self):
    #     cdec._parse_station_comments_table(table)

    # def test__parse_data_available(self):
    #     cdec._parse_data_available(text)

    # def test_get_data(self):
    #     cdec.get_data(station, start, end, sensor='', duration='')

    # def test_get_daily_snowpack_data(self):
    #     cdec.get_daily_snowpack_data(region, start, end)


# class TestCVO(unittest.TestCase):

    # def test(self):
    #     pass

    #     # prn test
    #     result = cvo.get_data(dt.date(2000, 2, 1), dt.date(2011, 3, 31), 'doutdly')

    #     # pdf test
    #     result = cvo.get_data(dt.date(2013, 12, 1), dt.date(2014, 1, 31), 'doutdly')
    #     result = cvo.get_data(dt.date(2000, 2, 1), dt.date(2023, 5, 1), 'shafln')
    #     result = cvo.get_data(dt.date(2012, 6, 1), dt.date(2013, 12, 31), 'slunit')
    #     result = cvo.get_data(dt.date(2020, 6, 1), dt.date(2021, 1, 1), 'fedslu')
    #     result = cvo.get_data(dt.date(2021, 1, 10), dt.date.now(), 'shadop')
    #     result = cvo.get_data(dt.date(2023, 5, 1), dt.date.now(), 'kesdop')

    # def test_get_area(self):
    #     cvo.get_area(date_structure, report_type)
    
    # def test_get_data(self):
    #     cvo.get_data(start, end, report_type)
    
    # def test_get_date_published(self):
    #     cvo.get_date_published(url, date_structure, report_type)
    
    # def test_get_report_columns(self):
    #     cvo.get_report_columns(report_type, date_structure, expected_length=None, default=False)
    
    # def test_get_report(self):
    #     cvo.get_report(date_structure, report_type)
    
    # def test_get_title(self):
    #     cvo.get_title(report_type)
    
    # def test_get_url(self):
    #     cvo.get_url(date_structure, report_type)
    
    # def test_months_between(self):
    #     cvo.months_between(start_date, end_date)
    
    # def test_doutdly_data_cleaner(self):
    #     cvo.doutdly_data_cleaner(content, report_type, date_structure)
    
    # def test_load_pdf_to_dataframe(self):
    #     cvo.load_pdf_to_dataframe(content, date_structure, report_type, to_csv=False)
    
    # def test_download_files(self):
    #     cvo.download_files(start, end, report_type, destination='.')


# class TestNID(unittest.TestCase):

    # def test_get_sites(self):
    #     nid.get_sites()

    # def test_get_issue_date(self):
    #     nid.get_issue_date()

    # def test_get_site_files(self):
    #     nid.get_site_files(site)

    # def test_get_site_metric(self):
    #     nid.get_site_metric(site, interval='daily')

    # def test_get_station_url(self):
    #     nid.get_station_url(site, metric='index', interval=None)

    # def test_get_daily_data(self):
    #     nid.get_daily_data(site, json_compatible=False)

    # def test_get_daily_meta(self):
    #     nid.get_daily_meta(url=None, content=None)

    # def test_get_hourly_data(self):
    #     nid.get_hourly_data(site, json_compatible=False)

    # def test_parse_qualifiers(self):
    #     nid.parse_qualifiers(series)

    # def test_serialize(self):
    #     nid.serialize(df, day_format='%Y-%-m-%-d')


# class TestSWP(unittest.TestCase):

    # def test_prompt_installation_and_exit(self):
    #     swp.prompt_installation_and_exit()

    # def test_get_report_catalog(self):
    #     swp.get_report_catalog()

    # def test_get_report_url(self):
    #     swp.get_report_url()

    # def test_get_raw_text(self):
    #     swp.get_raw_text()

    # def test_get_delta_daily_data(self):
    #     swp.get_delta_daily_data()

    # def test_get_barker_slough_data(self):
    #     swp.get_barker_slough_data()

    # def test_get_oco_tabular_data(self):
    #     swp.get_oco_tabular_data()


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
