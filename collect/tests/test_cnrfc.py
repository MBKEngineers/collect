"""
collect.tests.test_cnrfc
============================================================
initial test suite for collect.cnrfc data access and utility functions; note: these tests require internet connection
"""
import datetime as dt
import io
import os
import re
import textwrap
import unittest
import unittest.mock

from bs4 import BeautifulSoup
import pandas as pd

from collect import cnrfc, utils


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
                                                    float_precision='high').mul(1000)
        return self._deterministic_frame

    @unittest.mock.patch.dict(os.environ, {'CNRFC_USER': 'mock_user', 'CNRFC_PASSWORD': 'mock_password'}, clear=True)
    def test_cnrfc_credentials(self):
        """
        test that CNRFC credentials are read from the CNRFC_USER and CNRFC_PASSWORD environment variables
        """
        auth = cnrfc.cnrfc._get_cnrfc_auth('https://www.cnrfc.noaa.gov/restricted/graphicalRVF_csv.php?id=FOLC1')
        self.assertEqual((auth.username, auth.password), ('mock_user', 'mock_password'))

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

    def test_get_deterministic_forecast_watershed(self):
        """
        test watershed deterministic forecast download for North San Joaquin on a particular date;
        additional future tests to add coverage for arguments:
            - watershed
            - date_string
            - acre_feet=False
            - pdt_convert=False
            - as_pdt=False
            - cnrfc_id=None
        """
        df = cnrfc.get_deterministic_forecast_watershed('N_SanJoaquin', '2019040412')['data']
        self.assertEqual(df.head(20)['NHGC1'].values.tolist(),
                         self.deterministic_frame.head(20)['NHGC1'].values.tolist())
        self.assertIsNone(df.index.tzinfo)

    def test_get_water_year_trend_tabular(self):
        """
        test water year trend tabular download for a past year for Folsom reservoir forecast point
        """
        df = cnrfc.get_water_year_trend_tabular('FOLC1', '2022')['data']
        self.assertEqual(df.shape, (365, 9))

    def test_get_seasonal_trend_tabular(self):
        """
        test seasonal trend tabular download for a past year for Shasta reservoir forecast point
        """
        df = cnrfc.get_seasonal_trend_tabular('SHDC1', 2022)['data']
        self.assertEqual(df.shape, (365, 10))

    def test_get_ensemble_forecast(self):
        """
        test for current ensemble forecast file schema, using Vernalis forecast location
        """
        result = cnrfc.get_ensemble_forecast('VNSC1', 'hourly', acre_feet=False, pdt_convert=False, as_pdt=False)
        self.assertEqual(result['data'].shape[0], 721)
        self.assertTrue(result['data'].shape[1] > 40)
        self.assertIsNone(result['data'].index.tzinfo)
        self.assertEqual(result['info']['watershed'], 'SanJoaquin')
        self.assertEqual(result['info']['units'], 'cfs')

    def test_get_ensemble_product_1(self):
        """
        as this method is not yet implemented in the cnrfc module, it is expected to raise an error
        """
        self.assertRaises(NotImplementedError, cnrfc.get_ensemble_product_1, 'ORDC1')

    def test_get_ensemble_product_3(self):
        """
        as this method is not yet implemented in the cnrfc module, it is expected to raise an error
        """
        self.assertRaises(NotImplementedError, cnrfc.get_ensemble_product_3, 'ORDC1')

    def test_get_ensemble_product_5(self):
        """
        as this method is not yet implemented in the cnrfc module, it is expected to raise an error
        """
        self.assertRaises(NotImplementedError, cnrfc.get_ensemble_product_5, 'ORDC1')

    def test_get_ensemble_product_11(self):
        """
        as this method is not yet implemented in the cnrfc module, it is expected to raise an error
        """
        self.assertRaises(NotImplementedError, cnrfc.get_ensemble_product_11, 'ORDC1')

    def test_get_ensemble_product_12(self):
        """
        as this method is not yet implemented in the cnrfc module, it is expected to raise an error
        """
        self.assertRaises(NotImplementedError, cnrfc.get_ensemble_product_12, 'ORDC1')

    def test_get_ensemble_product_13(self):
        """
        as this method is not yet implemented in the cnrfc module, it is expected to raise an error
        """
        self.assertRaises(NotImplementedError, cnrfc.get_ensemble_product_13, 'ORDC1')

    def test_get_data_report_part_8(self):
        """
        as this method is not yet implemented in the cnrfc module, it is expected to raise an error
        """
        self.assertRaises(NotImplementedError, cnrfc.get_data_report_part_8)

    def test_get_monthly_reservoir_storage_summary(self):
        """
        as this method is not yet implemented in the cnrfc module, it is expected to raise an error
        """
        self.assertRaises(NotImplementedError, cnrfc.get_monthly_reservoir_storage_summary)

    def test_get_rating_curve(self):
        """
        example expected output from get_rating_curve method; assertion values updated 2026-07-28
        """
        result = cnrfc.get_rating_curve('DCSC1')
        self.assertEqual(result['data'][0], (1.02, 0.82))
        self.assertEqual(result['data'][-1], (15.0, 16300.0))
        self.assertEqual(result['info']['url'], 'https://www.cnrfc.noaa.gov/data/ratings/DCSC1_rating.js')

    def test_get_gage_group(self):
        """
        test that forecast location groups come from the CNRFC map location lists and combine without duplicates
        """
        river = cnrfc.gages.get_gage_group(rvf=True, inflow=False, release=False)
        inflow = cnrfc.gages.get_gage_group(rvf=False, inflow=True, release=False)
        release = cnrfc.gages.get_gage_group(rvf=False, inflow=False, release=True)
        ensemble = cnrfc.gages.get_gage_group(rvf=False, inflow=False, release=False, ensemble=True)

        # river-only, inflow-only, and release-only locations, and a location in all three groups
        self.assertIn('NMFC1', river)
        self.assertNotIn('NMFC1', inflow)
        self.assertIn('FOLC1', inflow)
        self.assertNotIn('FOLC1', river)
        self.assertIn('HAWC1', release)
        self.assertNotIn('HAWC1', inflow)
        for group in (river, inflow, release):
            self.assertIn('CLKC1', group)
        self.assertIn('ADLC1', ensemble)

        # default group is the river forecast points
        self.assertEqual(cnrfc.gages.get_gage_group(), river)

        # multiple groups combine into a sorted union without duplicates
        combined = cnrfc.gages.get_gage_group(rvf=True, release=True)
        self.assertEqual(combined, sorted(set(river) | set(release)))
        self.assertEqual(len(combined), len(set(combined)))
        self.assertEqual(cnrfc.gages.get_gage_group(rvf=False, release=False), [])

    def test_gage_group_snapshots(self):
        """
        test that the static gage group snapshots in collect.cnrfc.gages match the current CNRFC location lists;
        on failure, refresh the listed constant from get_gage_group
        """
        snapshots = {'RIVER_FORECAST': {'rvf': True},
                     'RESERVOIR_INFLOW': {'inflow': True},
                     'RESERVOIR_RELEASE': {'release': True},
                     'ENSEMBLE_FORECAST': {'ensemble': True}}
        for constant, selected in snapshots.items():
            with self.subTest(constant=constant):
                flags = {'rvf': False, 'inflow': False, 'release': False, 'ensemble': False, **selected}
                live = set(cnrfc.gages.get_gage_group(**flags))
                static = set(getattr(cnrfc.gages, constant))
                self.assertEqual(static, live,
                                 f'{constant} is out of date: added upstream {sorted(live - static)}, '
                                 f'removed upstream {sorted(static - live)}')

    def test_get_watershed(self):
        """
        example usage for looking up watershed group by forecast point ID
        """
        self.assertEqual(cnrfc.get_watershed('NCOC1'), 'LowerSacramento')

    def test_get_forecast_meta_deterministic(self):
        """
        test for predicted response with get_forecast_meta_deterministic for Oroville forecast point
        """
        result = cnrfc.get_forecast_meta_deterministic('ORDC1', first_ordinate=False, product='inflow')
        self.assertIsInstance(result[0], (dt.date, dt.datetime))
        self.assertIsInstance(result[1], (dt.date, dt.datetime))
        self.assertEqual(result[2], 'FEATHER RIVER - LAKE OROVILLE (ORDC1)')
        self.assertEqual(result[3], 'Impaired Inflows')

    @unittest.mock.patch.dict(os.environ, {}, clear=True)
    def test_get_forecast_meta_deterministic_product_pages(self):
        """
        test metadata from the river (graphicalRVF.php), reservoir inflow (reservoir.php), and reservoir release
        (reservoirRelease.php) product pages
        """
        # river and inflow forecasts at the same location have different titles
        river = cnrfc.get_forecast_meta_deterministic('BDBC1')
        self.assertEqual(river[2], 'SACRAMENTO RIVER - BEND BRIDGE (BDBC1)')
        inflow = cnrfc.get_forecast_meta_deterministic('BDBC1', product='inflow')
        self.assertEqual(inflow[2], 'SACRAMENTO RIVER - BEND BRIDGE LOCAL FLOW (BDBC1)')
        self.assertEqual(inflow[3], 'Impaired Inflows')

        # release metadata is available from the product page
        release = cnrfc.get_forecast_meta_deterministic('ORDC1', product='release')
        self.assertIsInstance(release[0], dt.datetime)
        self.assertIsInstance(release[1], dt.datetime)
        self.assertEqual(release[2], 'FEATHER RIVER - LAKE OROVILLE (ORDC1)')

    @staticmethod
    def _tabular_first_ordinate(route, cnrfc_id):
        """
        first forecast timestep listed in a CNRFC tabular data page, e.g. graphicalRVF-inflows_tabular.php
        """
        url = f'https://www.cnrfc.noaa.gov/{route}_tabular.php?id={cnrfc_id}'
        data = BeautifulSoup(utils.get_session_response(url).content, 'html.parser').find('pre').text.split('\n')
        return dt.datetime.strptime(data[data.index('# FORECAST') + 4].strip()[:16], '%m/%d/%Y %I %p')

    def _assert_first_ordinate_matches(self, route, cnrfc_id, **kwargs):
        """
        compare the first forecast timestep from the forecast CSV with the tabular data page
        """
        csv_first = cnrfc.get_forecast_meta_deterministic(cnrfc_id, first_ordinate=True, **kwargs)[-1]
        self.assertEqual(csv_first, self._tabular_first_ordinate(route, cnrfc_id))

    def test_first_ordinate_river(self):
        """
        test that the first forecast timestep from the river forecast CSV matches graphicalRVF_tabular.php
        """
        for cnrfc_id in ['VWBC1', 'VNSC1', 'TISC1']:
            with self.subTest(cnrfc_id=cnrfc_id):
                self._assert_first_ordinate_matches('graphicalRVF', cnrfc_id)

    def test_first_ordinate_inflow(self):
        """
        test that the first forecast timestep from the reservoir inflow forecast CSV matches
        graphicalRVF-inflows_tabular.php
        """
        for cnrfc_id in ['ORDC1', 'FOLC1', 'CFWC1']:
            with self.subTest(cnrfc_id=cnrfc_id):
                self._assert_first_ordinate_matches('graphicalRVF-inflows', cnrfc_id, product='inflow')
        with self.subTest(cnrfc_id='BDBC1', product='inflow'):
            self._assert_first_ordinate_matches('graphicalRVF-inflows', 'BDBC1', product='inflow')

    @unittest.expectedFailure
    def test_first_ordinate_release(self):
        """
        test that the first forecast timestep from the reservoir release forecast CSV matches
        graphicalRelease_tabular.php; expected to fail while graphicalRelease_tabular.php returns HTTP 500 upstream
        (since at least 2026-11-24) and the release CSV 'Date/Time' header is unsupported
        """
        for cnrfc_id in ['COYC1', 'NBBC1', 'ORDC1', 'PFTC1', 'EXQC1']:
            with self.subTest(cnrfc_id=cnrfc_id):
                self._assert_first_ordinate_matches('graphicalRelease', cnrfc_id, product='release')

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

    def test_get_watershed_forecast_issue_time(self):
        # test for the long range ensemble product
        self.assertIsInstance(cnrfc.get_watershed_forecast_issue_time('daily',
                                                                      'North Coast',
                                                                      date_string=None,
                                                                      deterministic=False), dt.datetime)

        # test for the hourly deterministic product
        self.assertIsInstance(cnrfc.get_watershed_forecast_issue_time('hourly',
                                                                      'North Coast',
                                                                      date_string=None,
                                                                      deterministic=True), dt.datetime)

        # the value None is returned for a specified forecast issuance in the past
        self.assertIsNone(cnrfc.get_watershed_forecast_issue_time('daily',
                                                                  'North Coast',
                                                                  date_string='2023010112',
                                                                  deterministic=False))

    def test__default_date_string(self):
        result = cnrfc.cnrfc._default_date_string(None)
        result_dt = dt.datetime.strptime(result, '%Y%m%d%H')
        self.assertIsInstance(result, str)
        self.assertIsInstance(result_dt, dt.datetime)
        self.assertIn(result_dt.hour, [0, 6, 12, 18])
        self.assertEqual(cnrfc.cnrfc._default_date_string('2023112818'), '2023112818')
        self.assertRaises(ValueError, cnrfc.cnrfc._default_date_string, '2023112805')

    def test_get_ensemble_product_url(self):
        self.assertEqual(cnrfc.get_ensemble_product_url(1, 'VNSC1', data_format=''),
                         'https://www.cnrfc.noaa.gov/ensembleProduct.php?id=VNSC1&prodID=1')
        self.assertEqual(cnrfc.get_ensemble_product_url(3, 'VNSC1', data_format=''),
                         'https://www.cnrfc.noaa.gov/ensembleProduct.php?id=VNSC1&prodID=3')
        self.assertEqual(cnrfc.get_ensemble_product_url(7, 'SHDC1', data_format='Tabular'),
                        'https://www.cnrfc.noaa.gov/ensembleProductTabular.php?id=SHDC1&prodID=7')

    def test_get_ensemble_product_6(self):
        """
        test download and parsing of monthly probability rainbow barchart plot for Shasta location
        """
        result = cnrfc.get_ensemble_product_6('SHDC1')
        self.assertEqual(result['data'].shape, (7, 12))
        self.assertEqual(result['data'].index.tolist(), ['10%', '25%', '50%', '75%', '90%', 'Mean', '%Mean'])
        self.assertEqual(result['info']['url'], 'https://www.cnrfc.noaa.gov/ensembleProduct.php?id=SHDC1&prodID=6')
        self.assertEqual(result['info']['type'], 'Monthly Streamflow Volume (1000s of Acre-Feet)')
        self.assertEqual(result['info']['units'], 'TAF')

    def test_get_ensemble_product_10(self):
        """
        test download and parsing of water year accumulated volume plot for Shasta location
        """
        result = cnrfc.get_ensemble_product_10('SHDC1')
        self.assertEqual(result['data'].shape, (5, 12))
        self.assertEqual(result['data'].index.tolist(), ['10%', '25%', '50%(Median)', '75%', '90%'])
        self.assertEqual(result['info']['url'], 'https://www.cnrfc.noaa.gov/ensembleProduct.php?id=SHDC1&prodID=10')
        self.assertEqual(result['info']['type'],
            'Water Year Accumulated Volume Plot & Tabular Monthly Volume Accumulation')
        self.assertEqual(result['info']['units'], 'TAF')

    def test__parse_blue_table(self):
        """
        test the processing of included data table for monthly summary associated with ensemble products like 2, 10, etc
        """
        table_soup = BeautifulSoup(io.StringIO(textwrap.dedent("""/
            <table border="0" cellpadding="0" style="standardTable" width="100%">
            <tr bgcolor="#003399">
            <td align="center" class="medBlue-background" colspan="11" valign="middle"><strong>Title</strong></td>
            </tr>
            <tr>
            <td align="center" class="blue-background" width="20%"><b>Probability</b></td>
            <td align="center" class="blue-background" width="8%"><b>Nov<br/>29</b></td>
            <td align="center" class="blue-background" width="8%"><b>Nov<br/>30</b></td>
            <td align="center" class="blue-background" width="8%"><b>Dec<br/>01</b></td>
            <td align="center" class="blue-background" width="8%"><b>Dec<br/>02</b></td>
            <td align="center" class="blue-background" width="8%"><b>Dec<br/>03</b></td>
            </tr>
            <tr>
            <td align="center" bgcolor="#999999" class="normalText" height="33" width="20%">
                <font color="#0033FF"><b>10%</b></font></td>
            <td align="center" bgcolor="#CCCCCC" class="normalText" width="8%">12.2</td>
            <td align="center" bgcolor="#CCCCCC" class="normalText" width="8%">24.4</td>
            <td align="center" bgcolor="#CCCCCC" class="normalText" width="8%">36.7</td>
            <td align="center" bgcolor="#CCCCCC" class="normalText" width="8%">49.0</td>
            <td align="center" bgcolor="#CCCCCC" class="normalText" width="8%">62.5</td>
            </tr>
            <tr>
            <td align="center" bgcolor="#999999" class="normalText" height="33" width="20%">
                <font color="#00FFFF"><b>25%</b></font></td>
            <td align="center" bgcolor="#CCCCCC" class="normalText" width="8%">12.2</td>
            <td align="center" bgcolor="#CCCCCC" class="normalText" width="8%">24.4</td>
            <td align="center" bgcolor="#CCCCCC" class="normalText" width="8%">36.7</td>
            <td align="center" bgcolor="#CCCCCC" class="normalText" width="8%">48.9</td>
            <td align="center" bgcolor="#CCCCCC" class="normalText" width="8%">61.4</td>
            </tr>
            <tr>
            <td align="center" bgcolor="#999999" class="normalText" height="33" width="20%"><b>
                <font color="#33FF33">50%<br/>(Median)</font></b></td>
            <td align="center" bgcolor="#CCCCCC" class="normalText" width="8%">12.2</td>
            <td align="center" bgcolor="#CCCCCC" class="normalText" width="8%">24.4</td>
            <td align="center" bgcolor="#CCCCCC" class="normalText" width="8%">36.6</td>
            <td align="center" bgcolor="#CCCCCC" class="normalText" width="8%">48.9</td>
            <td align="center" bgcolor="#CCCCCC" class="normalText" width="8%">61.2</td>
            </tr>
            </table>
        """)), 'html.parser')
        result = cnrfc.cnrfc._parse_blue_table(table_soup)
        self.assertIsInstance(result[0], pd.DataFrame)
        self.assertEqual(result[0]['Probability'].tolist(), ['10%', '25%', '50%(Median)'])
        self.assertIsInstance(result[1], list)

    def test__apply_conversions(self):
        """
        test application of UTC->PST/PDT and kcfs->cfs or kcfs->acre-feet unit conversions for a sample ensemble
        """
        df = pd.DataFrame(data=[[0.111, 0.222, 0.333]] * 6,
                          index=pd.date_range('2023-11-01 12:00:00', periods=6, freq='h'))

        # test for conversion of kcfs -> acre-feet, no timezone handling
        result = cnrfc.cnrfc._apply_conversions(df, 'hourly', True, False, False)
        self.assertIsInstance(result[0], pd.DataFrame)
        self.assertEqual(pd.to_datetime(result[0].first_valid_index()), dt.datetime(2023, 11, 1, 12, tzinfo=None))
        self.assertEqual(round(result[0].loc[result[0].first_valid_index(), 0], 6), 9.173554)
        self.assertEqual(result[1], 'acre-feet')

        # reset test frame
        df = pd.DataFrame(data=[[0.111, 0.222, 0.333]] * 6,
                          index=pd.date_range('2023-11-01 12:00:00', periods=6, freq='h'))

        # test for conversion of timezone and kcfs -> cfs
        result = cnrfc.cnrfc._apply_conversions(df, 'hourly', False, True, True)
        self.assertIsInstance(result[0], pd.DataFrame)

        # create a localized datetime with either pytz or zoneinfo modules
        expected_dt = utils.get_localized_datetime(dt.datetime(2023, 11, 1, 5), 'America/Los_Angeles')
        self.assertEqual(result[0].first_valid_index().to_pydatetime(), expected_dt)
        self.assertEqual(result[1], 'cfs')

    def test_get_ensemble_forecast_watershed(self):
        """
        test for retrieiving an ensemble forecast watershed file for a forecast issuance prior to most recent
        """
        result = cnrfc.get_ensemble_forecast_watershed('SalinasPajaro',
                                                       'hourly',
                                                       '2023010118',
                                                       acre_feet=False,
                                                       pdt_convert=False,
                                                       as_pdt=False,
                                                       cnrfc_id=None)
        self.assertEqual(result['data'].shape, (721, 924))
        self.assertEqual(result['data'].tail(1)['BTEC1'].values[0], 226.94)
        self.assertEqual(pd.to_datetime(result['data'].last_valid_index()), dt.datetime(2023, 1, 31, 18, 0, 0))
        self.assertEqual(result['info']['watershed'], 'SalinasPajaro')
        self.assertEqual(result['info']['url'],
                         'https://www.cnrfc.noaa.gov/csv/2023010118_SalinasPajaro_hefs_csv_hourly.zip')
        self.assertIsNone(result['info']['issue_time'])

    def test_get_esp_trace_analysis_url(self):
        """
        test that the build-your-own trace analysis product url is properly constructed for the provided options
        """
        url = cnrfc.get_esp_trace_analysis_url('BTYO3',
                                               interval='day',
                                               value_type='mean',
                                               plot_type='traces',
                                               table_type='forecastInfo',
                                               product_type='table',
                                               date_string='20231106',
                                               end_date_string='20231231')
        expected_url = '&'.join(['https://www.cnrfc.noaa.gov/ensembleProduct.php?id=BTYO3',
                                 'prodID=8',
                                 'interval=day',
                                 'valueType=mean',
                                 'plotType=traces',
                                 'tableType=forecastInfo',
                                 'productType=table',
                                 'dateSelection=custom',
                                 'date=20231106',
                                 'endDate=20231231'])
        self.maxDiff = 800
        self.assertEqual(url, expected_url)

    def test_get_ensemble_first_forecast_ordinate(self):
        """
        test that the first ensemble forecast ordinate is a datetime in the past
        """
        result = cnrfc.get_ensemble_first_forecast_ordinate(
            url='https://www.cnrfc.noaa.gov/csv/HLEC1_hefs_csv_hourly.csv',
            df=None
        )
        self.assertIsInstance(result, dt.datetime)
        result_utc = utils.get_localized_datetime(result, 'UTC')
        self.assertLess(result_utc, dt.datetime.now(dt.timezone.utc))

    def test__get_forecast_csv(self):
        """
        test for forecast CSV data retrieval to in-memory filelike object (private method)
        """
        result = cnrfc.cnrfc._get_forecast_csv('https://www.cnrfc.noaa.gov/csv/HLEC1_hefs_csv_hourly.csv')
        self.assertIsInstance(result, io.BytesIO)

        # check first line contains forecast point headers
        self.assertTrue(result.readline().decode('utf-8').startswith('GMT,HLEC1'))

        # check second line contains variables identifiers
        self.assertTrue(result.readline().decode('utf-8').startswith(',QINE,QINE'))

        # check third line starts with date/time of proper format and contains expected timeseries info
        pattern = r'^(\d{4}-\d{2}-\d{2} \d{2}:00:00),((\d+.\d+,)+)'
        self.assertTrue(len(re.match(pattern, result.readline().decode('utf-8')).groups()) > 2)

    def test_get_forecast_csvdata(self):
        """
        test for forecast CSV data retrieval to in-memory filelike object (public method); duplicate of
        test__get_forecast_csv
        """
        result = cnrfc.get_forecast_csvdata('https://www.cnrfc.noaa.gov/csv/HLEC1_hefs_csv_hourly.csv')
        self.assertIsInstance(result, io.BytesIO)
        self.assertTrue(result.readline().decode('utf-8').startswith('GMT,HLEC1'))
        self.assertTrue(result.readline().decode('utf-8').startswith(',QINE,QINE'))

        # check third line starts with date/time of proper format and contains expected timeseries info
        pattern = r'^(\d{4}-\d{2}-\d{2} \d{2}:00:00),((\d+.\d+,)+)'
        self.assertTrue(len(re.match(pattern, result.readline().decode('utf-8')).groups()) > 2)

    def test__get_cnrfc_content(self):
        """
        test that restricted content can be accessed through the provided credentials
        """
        result = cnrfc.cnrfc._get_cnrfc_content(
            'https://www.cnrfc.noaa.gov/restricted/graphicalRVF_tabular.php?id=FOLC1'
        )
        sample = BeautifulSoup(result, 'html.parser').find('pre').text.splitlines()[:9]
        self.assertEqual(sample[2], '# Location: American River - Folsom Lake (FOLC1)')
        self.assertTrue(sample[-1].startswith('# Maximum Observed Flow:'))

    @unittest.mock.patch.dict(os.environ, {}, clear=True)
    def test__get_cnrfc_auth_without_credentials(self):
        """
        test that credentials are not required for public URLs, but are required for restricted site URLs
        """
        self.assertIsNone(cnrfc.cnrfc._get_cnrfc_auth('https://www.cnrfc.noaa.gov/csv/HLEC1_hefs_csv_hourly.csv'))
        self.assertRaises(NotImplementedError,
                          cnrfc.cnrfc._get_cnrfc_auth,
                          'https://www.cnrfc.noaa.gov/restricted/graphicalRVF_csv.php?id=FOLC1')

    @unittest.mock.patch.dict(os.environ, {'CNRFC_USER': 'mock_user', 'CNRFC_PASSWORD': 'mock_password'}, clear=True)
    def test__get_cnrfc_auth_public_url(self):
        """
        test that credentials are not attached to public URLs even when available
        """
        self.assertIsNone(cnrfc.cnrfc._get_cnrfc_auth('https://www.cnrfc.noaa.gov/csv/HLEC1_hefs_csv_hourly.csv'))

    @unittest.mock.patch.dict(os.environ, {}, clear=True)
    def test__get_forecast_csv_without_credentials(self):
        """
        test that public forecast CSVs can be retrieved without CNRFC credentials
        """
        result = cnrfc.cnrfc._get_forecast_csv('https://www.cnrfc.noaa.gov/csv/HLEC1_hefs_csv_hourly.csv')
        self.assertTrue(result.readline().decode('utf-8').startswith('GMT,HLEC1'))

    def test_get_deterministic_forecast_public(self):
        """
        test deterministic forecast for a location previously served on the restricted site
        """
        result = cnrfc.get_deterministic_forecast('NMFC1')
        self.assertEqual(result['info']['url'], 'https://www.cnrfc.noaa.gov/graphicalRVF_csv.php?id=NMFC1')
        self.assertGreater(result['data']['forecast'].dropna().size, 0)

    @unittest.mock.patch.dict(os.environ, {}, clear=True)
    def test_get_deterministic_forecast_inflows(self):
        """
        test deterministic forecast for a reservoir inflow location formerly on the restricted site, which is now
        served on the public "-inflows" route without credentials
        """
        result = cnrfc.get_deterministic_forecast('ORDC1', product='inflow')
        self.assertEqual(result['info']['url'], 'https://www.cnrfc.noaa.gov/graphicalRVF-inflows_csv.php?id=ORDC1')
        self.assertGreater(result['data']['forecast'].dropna().size, 0)
        self.assertEqual(result['info']['title'], 'FEATHER RIVER - LAKE OROVILLE (ORDC1)')

    def test__get_product_spec(self):
        """
        test that each deterministic forecast product has routes and a CSV layout, and unknown products are rejected
        """
        for product in ['rvf', 'inflow', 'release']:
            with self.subTest(product=product):
                spec = cnrfc.cnrfc._get_product_spec(product)
                self.assertTrue(spec['csv'].endswith('_csv.php'))
                self.assertTrue(spec['page'].endswith('.php'))
                self.assertIn(spec['date_column'], ('Valid Date/Time (Pacific)', 'Date/Time (Pacific Time)', 'Date/Time'))
        for product in ['river', 'RVF', None, True]:
            with self.subTest(product=product):
                self.assertRaises(ValueError, cnrfc.cnrfc._get_product_spec, product)

    def test__raise_if_invalid_id(self):
        """
        test that CNRFC's invalid ID response is reported as a ValueError naming the location and URL
        """
        url = 'https://www.cnrfc.noaa.gov/graphicalRelease_csv.php?id=NMFC1'
        with self.assertRaisesRegex(ValueError, r'CNRFC does not serve this forecast product for NMFC1'):
            cnrfc.cnrfc._raise_if_invalid_id('NMFC1', url, b'ERROR: Invalid ID Entered (Value Given: NMFC1)')
        cnrfc.cnrfc._raise_if_invalid_id('ORDC1', url, b'Date/Time,Release Flow (CFS),Trend\n')

    @unittest.mock.patch.dict(os.environ, {}, clear=True)
    def test_get_deterministic_forecast_unavailable_product(self):
        """
        test the messages for the ways CNRFC answers a request for a product it doesn't serve at a location
        """
        cases = [
            # "Invalid ID" with HTTP 200: no inflow forecast at a release-only location
            ('HAWC1', 'inflow', r'does not serve this forecast product for HAWC1'),
            # "Invalid ID" with HTTP 200: no release forecast at a river-only location
            ('NMFC1', 'release', r'does not serve this forecast product for NMFC1'),
            # "Invalid ID" with HTTP 400: unknown code on the river forecast route
            ('ZZZZ1', 'rvf', r'does not serve this forecast product for ZZZZ1'),
            # column headers without rows: no river forecast at an inflow-only location
            ('FOLC1', 'rvf', r'no forecast data for FOLC1'),
        ]
        for cnrfc_id, product, message in cases:
            with self.subTest(cnrfc_id=cnrfc_id, product=product):
                with self.assertRaisesRegex(ValueError, message):
                    cnrfc.get_deterministic_forecast(cnrfc_id, product=product)

        # product pages answer "Invalid ID" too
        with self.assertRaisesRegex(ValueError, r'does not serve this forecast product for FOLC1'):
            cnrfc.get_forecast_meta_deterministic('FOLC1')

    @unittest.mock.patch.dict(os.environ, {}, clear=True)
    def test_get_deterministic_forecast_river_and_inflow(self):
        """
        test that a location with both river and reservoir inflow forecasts returns the river forecast by default
        and the inflow forecast on request
        """
        river = cnrfc.get_deterministic_forecast('BDBC1')
        self.assertEqual(river['info']['url'], 'https://www.cnrfc.noaa.gov/graphicalRVF_csv.php?id=BDBC1')

        # river forecast returns flow and stage series, split into forecast and historical, when the CSV has both
        for column in ['Flow (CFS)', 'Stage (Feet)', 'forecast', 'historical', 'forecast_stage', 'historical_stage']:
            with self.subTest(column=column):
                self.assertIn(column, river['data'].columns)
                self.assertGreater(river['data'][column].dropna().size, 0)
        self.assertEqual(river['info']['units'], 'cfs')
        self.assertEqual(river['info']['units_stage'], 'feet')

        inflow = cnrfc.get_deterministic_forecast('BDBC1', product='inflow')
        self.assertEqual(inflow['info']['url'], 'https://www.cnrfc.noaa.gov/graphicalRVF-inflows_csv.php?id=BDBC1')
        self.assertEqual(inflow['info']['type'], 'Deterministic Inflow Forecast')
        self.assertNotIn('forecast_stage', inflow['data'].columns)
        self.assertNotIn('units_stage', inflow['info'])
        self.assertGreater(inflow['data']['forecast'].dropna().size, 0)

        # flow and stage forecasts start at the same timestep
        self.assertEqual(river['data']['forecast'].first_valid_index(),
                         river['data']['forecast_stage'].first_valid_index())

    @unittest.mock.patch.dict(os.environ, {}, clear=True)
    def test_get_deterministic_forecast_stage_only(self):
        """
        test a stage-only river forecast location (CLKC1, Clear Lake), whose CSV has no flow column and a blank Trend
        column; the forecast start comes from the Observed/Forecast column
        """
        result = cnrfc.get_deterministic_forecast('CLKC1')
        for column in ['forecast_stage', 'historical_stage']:
            with self.subTest(column=column):
                self.assertGreater(result['data'][column].dropna().size, 0)
        for absent in ['forecast', 'historical']:
            with self.subTest(absent=absent):
                self.assertNotIn(absent, result['data'].columns)
        self.assertEqual(result['info']['units_stage'], 'feet')
        self.assertNotIn('units', result['info'])

        # forecast starts at the first 'Forecast' row, not the last row
        first = result['data']['forecast_stage'].first_valid_index()
        self.assertEqual(result['info']['first_ordinate'], first.strftime('%Y-%m-%d %H:%M'))
        self.assertLess(first, result['data'].index.max())
        self.assertEqual(result['data'].loc[first, 'Observed/Forecast'], 'Forecast')

    @unittest.mock.patch.dict(os.environ, {}, clear=True)
    def test_get_deterministic_forecast_release(self):
        """
        test reservoir release forecasts from graphicalRelease_csv.php ('Date/Time' and 'Release Flow (CFS)' columns)
        with metadata from reservoirRelease.php
        """
        for cnrfc_id in ['COYC1', 'NBBC1', 'ORDC1', 'PFTC1', 'EXQC1']:
            with self.subTest(cnrfc_id=cnrfc_id):
                result = cnrfc.get_deterministic_forecast(cnrfc_id, product='release')
                self.assertEqual(result['info']['url'],
                                 f'https://www.cnrfc.noaa.gov/graphicalRelease_csv.php?id={cnrfc_id}')
                self.assertEqual(result['info']['type'], 'Deterministic Release Forecast')
                self.assertTrue(result['info']['title'].endswith(f'({cnrfc_id})'))
                self.assertIn('Release Flow (CFS)', result['data'].columns)
                self.assertGreater(result['data']['forecast'].dropna().size, 0)
                self.assertGreater(result['data']['historical'].dropna().size, 0)

    @unittest.mock.patch('collect.cnrfc.cnrfc._get_forecast_csv',
                         return_value=io.BytesIO(b'Timestamp,Release (CFS)\n11/24/2026 02 PM,100\n'))
    def test_get_deterministic_forecast_unexpected_layout(self, mock_csv):
        """
        test that a CSV without the expected date, value, and trend columns raises a ValueError naming the columns
        """
        with self.assertRaisesRegex(ValueError, r"expected \['Date/Time', 'Trend', 'Release Flow \(CFS\)'\]"):
            cnrfc.get_deterministic_forecast('ORDC1', product='release')
        mock_csv.assert_called_once()

    def test_get_deterministic_forecast_product_names(self):
        """
        test that the forecast and its metadata reject unknown products before any request is made
        """
        for product in ['river', 'reservoir', None]:
            for method in (cnrfc.get_deterministic_forecast, cnrfc.get_forecast_meta_deterministic):
                with self.subTest(method=method.__name__, product=product):
                    with self.assertRaisesRegex(ValueError, r"choose one of \['rvf', 'inflow', 'release'\]"):
                        method('ORDC1', product=product)

        # default river forecast is not available for an inflow-only location
        with self.subTest(cnrfc_id='FOLC1'):
            self.assertRaises(ValueError, cnrfc.get_deterministic_forecast, 'FOLC1')

    def test_download_watershed_file(self):
        """
        test for downloading watershed file to local file system (in this case, downloaded to in-memory object)
        """
        result, filename = cnrfc.download_watershed_file('WSI',
                                                         '2023010112',
                                                         'ensemble',
                                                         duration='daily',
                                                         return_content=True)
        self.assertIsInstance(result, io.BytesIO)
        self.assertIsInstance(filename, str)

        # check first line contains forecast point headers
        self.assertTrue(result.readline().decode('utf-8').startswith('GMT,SACC0'))

        # check second line contains variables identifiers
        self.assertTrue(result.readline().decode('utf-8').startswith(',SQME,SQME'))

        # check third line contains expected timeseries info
        self.assertTrue(result.readline().decode('utf-8').startswith('2023-01-01 12:00:00,252.83904,'))


if __name__ == '__main__':
    unittest.main()
