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

from collect import cnrfc


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

    # def test_cnrfc_credentials(self):
    #     """
    #     load sensitive info from .env file and test CNRFC credentials exist
    #     """
    #     load_dotenv()
    #     self.assertTrue(('CNRFC_USER' in os.environ) & ('CNRFC_PASSWORD' in os.environ))

    # def test_convert_date_columns(self):
    #     """Ensure datetime data converted to string format"""
    #     test_index = self.deterministic_frame.index.strftime('%Y-%m-%d')
    #     self.assertEqual(test_index.tolist()[0], '2019-03-30')

    # def test_validate_duration(self):
    #     """
    #     function to properly format/case hourly or daily durations
    #     """
    #     duration = 'Hourly'
    #     self.assertEqual(cnrfc.cnrfc._validate_duration(duration), 'hourly')

    # def test_validate_duration_invalid(self):
    #     """
    #     test that invalid duration raises a ValueError
    #     """
    #     bad_input = 'monthly'
    #     self.assertRaises(ValueError,
    #                       cnrfc.cnrfc._validate_duration,
    #                       bad_input)

    # def test_get_deterministic_forecast(self):
    #     """
    #     Test that deterministic forecast start from Graphical_RVF page matches
    #     CSV start of forecast
    #     """
    #     cnrfc_id = 'FOLC1'
    #     first_ordinate = cnrfc.get_forecast_meta_deterministic(cnrfc_id, first_ordinate=True)[-1]
    #     df = cnrfc.get_deterministic_forecast(cnrfc_id, truncate_historical=False)['data']
    #     first_forecast_entry = df['forecast'].dropna().index.tolist()[0]

    #     # check that the date/time representation in the timestamp and datetime.datetime objects are the same
    #     self.assertEqual(first_forecast_entry.year, first_ordinate.year)
    #     self.assertEqual(first_forecast_entry.month, first_ordinate.month)
    #     self.assertEqual(first_forecast_entry.day, first_ordinate.day)
    #     self.assertEqual(first_forecast_entry.hour, first_ordinate.hour)
    #     self.assertEqual(first_forecast_entry.minute, first_ordinate.minute)

    #     # for now, strip the local tzinfo from `first_ordinate`
    #     self.assertEqual(first_forecast_entry.tzinfo, first_ordinate.replace(tzinfo=None).tzinfo)

    # def test_get_deterministic_forecast_watershed(self):
    #     """
    #     test watershed deterministic forecast download for North San Joaquin on a particular date;
    #     additional future tests to add coverage for arguments:
    #         - watershed
    #         - date_string
    #         - acre_feet=False
    #         - pdt_convert=False
    #         - as_pdt=False
    #         - cnrfc_id=None
    #     """
    #     df = cnrfc.get_deterministic_forecast_watershed('N_SanJoaquin', '2019040412')['data']
    #     self.assertEqual(df.head(20)['NHGC1'].values.tolist(),
    #                      self.deterministic_frame.head(20)['NHGC1'].values.tolist())
    #     self.assertIsNone(df.index.tzinfo)

    # def test_get_water_year_trend_tabular(self):
    #     """
    #     test water year trend tabular download for a past year for Folsom reservoir forecast point
    #     """
    #     df = cnrfc.get_water_year_trend_tabular('FOLC1', '2022')['data']
    #     self.assertEqual(df.shape, (365, 9))

    # def test_get_seasonal_trend_tabular(self):
    #     """
    #     test seasonal trend tabular download for a past year for Shasta reservoir forecast point
    #     """
    #     df = cnrfc.get_seasonal_trend_tabular('SHDC1', 2022)['data']
    #     self.assertEqual(df.shape, (365, 10))

    # def test_get_ensemble_forecast(self):
    #     """
    #     test for current ensemble forecast file schema, using Vernalis forecast location
    #     """
    #     result = cnrfc.get_ensemble_forecast('VNSC1', 'hourly', acre_feet=False, pdt_convert=False, as_pdt=False)
    #     self.assertEqual(result['data'].shape, (721, 43))
    #     self.assertIsNone(result['data'].index.tzinfo)
    #     self.assertEqual(result['info']['watershed'], 'SanJoaquin')
    #     self.assertEqual(result['info']['units'], 'cfs')

    # def test_get_ensemble_product_1(self):
    #     self.assertRaises(NotImplementedError, cnrfc.get_ensemble_product_1, 'ORDC1')

    # def test_get_ensemble_product_3(self):
    #     self.assertRaises(NotImplementedError, cnrfc.get_ensemble_product_3, 'ORDC1')

    # def test_get_ensemble_product_5(self):
    #     self.assertRaises(NotImplementedError, cnrfc.get_ensemble_product_5, 'ORDC1')

    # def test_get_ensemble_product_11(self):
    #     self.assertRaises(NotImplementedError, cnrfc.get_ensemble_product_11, 'ORDC1')

    # def test_get_ensemble_product_12(self):
    #     self.assertRaises(NotImplementedError, cnrfc.get_ensemble_product_12, 'ORDC1')

    # def test_get_ensemble_product_13(self):
    #     self.assertRaises(NotImplementedError, cnrfc.get_ensemble_product_13, 'ORDC1')

    # def test_get_data_report_part_8(self):
    #     self.assertRaises(NotImplementedError, cnrfc.get_data_report_part_8)

    # def test_get_monthly_reservoir_storage_summary(self):
    #     self.assertRaises(NotImplementedError, cnrfc.get_monthly_reservoir_storage_summary)

    # def test_get_rating_curve(self):
    #     """
    #     example expected output from get_rating_curve method
    #     """
    #     result = cnrfc.get_rating_curve('DCSC1')
    #     self.assertEqual(result['data'][0], (0.92, 0.45))
    #     self.assertEqual(result['data'][-1], (15.0, 16300.0))
    #     self.assertEqual(result['info']['url'], 'https://www.cnrfc.noaa.gov/data/ratings/DCSC1_rating.js')

    # def test_get_watershed(self):
    #     """
    #     example usage for looking up watershed group by forecast point ID
    #     """
    #     self.assertEqual(cnrfc.get_watershed('NCOC1'), 'LowerSacramento')

    # def test_get_forecast_meta_deterministic(self):
    #     """
    #     test for predicted response with get_forecast_meta_deterministic for Oroville forecast point
    #     """
    #     result = cnrfc.get_forecast_meta_deterministic('ORDC1', first_ordinate=False, release=False)
    #     self.assertTrue(isinstance(result[0], (dt.date, dt.datetime)))
    #     self.assertTrue(isinstance(result[1], (dt.date, dt.datetime)))
    #     self.assertEqual(result[2], 'FEATHER RIVER - LAKE OROVILLE (ORDC1)')
    #     self.assertEqual(result[3], 'Impaired Inflows')

    def deferred_test_get_ensemble_forecast_watershed(self):
        result = cnrfc.get_ensemble_forecast_watershed(watershed,
                                                       duration,
                                                       date_string,
                                                       acre_feet=False,
                                                       pdt_convert=False,
                                                       as_pdt=False,
                                                       cnrfc_id=None)

    def deferred_test_download_watershed_file(self):
        result = cnrfc.download_watershed_file(watershed, date_string, forecast_type, duration=None, path=None)

    def deferred_test_get_watershed_forecast_issue_time(self):
        result = cnrfc.get_watershed_forecast_issue_time(duration, watershed, date_string=None, deterministic=False)

    def deferred_test_get_ensemble_first_forecast_ordinate(self):
        result = cnrfc.get_ensemble_first_forecast_ordinate(url=None, df=None)

    def deferred_test_get_ensemble_product_url(self):
        result = cnrfc.get_ensemble_product_url(product_id, cnrfc_id, data_format='')

    def test_get_ensemble_product_2(self):
        """
        test for the expected format of ensemble produce #2
        """
        result = cnrfc.get_ensemble_product_2('BDBC1')
        self.assertEqual(result['info']['type'], 'Tabular 10-Day Streamflow Volume Accumulation')
        self.assertEqual(result['info']['units'], 'TAF')
        self.assertEqual(result['data'].shape, (6, 10))
        self.assertEqual(result['data'].index.tolist(),
                         ['10%', '25%', '50%(Median)', '75%', '90%', 'CNRFCDeterministic Forecast'])

    def deferred_test_get_ensemble_product_6(self):
        result = cnrfc.get_ensemble_product_6(cnrfc_id)

    def deferred_test_get_ensemble_product_10(self):
        result = cnrfc.get_ensemble_product_10(cnrfc_id)

    def deferred_test_esp_trace_analysis_wrapper(self):
        result = cnrfc.esp_trace_analysis_wrapper()

    def deferred_test__apply_conversions(self):
        result = cnrfc._apply_conversions(df, duration, acre_feet, pdt_convert, as_pdt)

    def deferred_test__get_cnrfc_restricted_content(self):
        result = cnrfc._get_cnrfc_restricted_content(url)

    def deferred_test__get_forecast_csv(self):
        result = cnrfc._get_forecast_csv(url)

    def deferred_test_get_forecast_csvdata(self):
        result = cnrfc.get_forecast_csvdata(url)

    def deferred_test__default_date_string(self):
        result = cnrfc._default_date_string(date_string)

    def deferred_test__parse_blue_table(self):
        result = cnrfc._parse_blue_table(table_soup)


if __name__ == '__main__':
    unittest.main()
