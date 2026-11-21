"""
collect.tests.test_usace
============================================================
initial test suite for collect.usace data access and utility functions; note: these tests require internet connection
"""
import datetime as dt
import textwrap
import unittest
from unittest import mock
from zoneinfo import ZoneInfo
from collect.usace import wcds


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

    def test_get_wcds_reservoirs(self):
        """
        show that 35 reservoirs exist in the internal collect record for WCDS reservoirs
        """
        self.assertEqual(wcds.get_wcds_reservoirs().shape[0], 35)

    def test_get_wcds_data(self):
        result = wcds.get_wcds_data('sha',
                                    dt.datetime(2023, 1, 15, tzinfo=ZoneInfo('US/Pacific')),
                                    dt.datetime(2023, 2, 1, tzinfo=ZoneInfo('US/Pacific')),
                                    interval='d')
        self.assertEqual(result['data'].shape, (18, 16))
        self.assertEqual(result['data']['Storage'].tolist()[:4], [2235532.0, 2308907.0, 2357517.0, 2392661.0])

    def test_get_release_report(self):
        self.assertEqual(wcds.get_release_report('buc')['info']['units'], 'cfs')
        self.assertGreater(wcds.get_release_report('buc')['data'].shape[0], 0)

    def test_get_reservoir_metadata(self):
        result = wcds.get_reservoir_metadata('nhg', 2022, interval='d')
        self.assertEqual(int(result['gross pool (stor)']), 317100)
        self.assertEqual(int(result['gross pool (elev)']), 713)
        self.assertTrue('Precip @ Dam (in; elev 712 ft)' in result['data headers'])

    def test_extract_basin_totals(self):
        result = wcds.extract_basin_totals(dt.datetime(2025, 1, 1, tzinfo=ZoneInfo('US/Pacific')))
        self.assertEqual(float(result.loc['BASIN TOTALS', 'Percent Encroached']), 5.0)
        # earlier format without percent encroached or the total flood space encroached row
        result = wcds.extract_basin_totals(dt.datetime(2015, 1, 1, tzinfo=ZoneInfo('US/Pacific')))
        self.assertEqual(result.loc['BASIN TOTALS'].tolist(),
                         [10469900.0, 7919960.0, 4238480.0, 40.0, -3681480.0, None])
        self.assertEqual(result.loc['w/US Storages'].tolist(), [11124200.0, None, 4563619.0, 41.0, None, None])

    def _extract_basin_totals_from_text(self, totals_rows, **kwargs):
        report = '\n'.join([
            '                           ---- Sacramento Valley ----',
            *totals_rows,
            '',
            ' **  Percent Encroached',
            '                           ---- San Joaquin Valley ----',
        ])
        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            mock_response.return_value.text = report
            return wcds.extract_basin_totals(dt.datetime(2025, 1, 1, tzinfo=ZoneInfo('US/Pacific')), **kwargs)

    def test_extract_basin_totals_not_reported(self):
        """
        not-reported (-NR-) totals are None by default or `nr_value` when given, and a missing row is omitted
        (report data ending 16 NOV 2014)
        """
        rows = [' BASIN TOTALS  10,469,900      -NR-      -NR-               -NR-       -NR-',
                ' w/US Storages 11,124,200                -NR-']
        result = self._extract_basin_totals_from_text(rows)
        self.assertEqual(result.index.tolist(), ['BASIN TOTALS', 'w/US Storages'])
        self.assertEqual(result.loc['BASIN TOTALS'].tolist(), [10469900.0] + [None] * 5)
        self.assertEqual(result.loc['w/US Storages'].tolist(), [11124200.0] + [None] * 5)

        # the blank % of gross pool stays None, so later -NR- values stay in their columns
        result = self._extract_basin_totals_from_text(rows, nr_value='NR')
        self.assertEqual(result.loc['BASIN TOTALS'].tolist(), [10469900.0, 'NR', 'NR', None, 'NR', None])
        self.assertEqual(result.loc['w/US Storages'].tolist(), [11124200.0, None, 'NR', None, None, None])

    def test_extract_basin_totals_zero(self):
        """
        zero totals are parsed as values (report data ending 22 JAN 2026)
        """
        result = self._extract_basin_totals_from_text([
            ' BASIN TOTALS  10,469,900         0         0   0              0(  0)',
            ' TOTAL FLOOD SPACE ENCROACHED                                  0',
            ' w/US Storages 11,124,200                   0   0',
        ])
        self.assertEqual(result.loc['BASIN TOTALS'].tolist(), [10469900.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        self.assertEqual(result.loc['TOTAL FLOOD SPACE ENCROACHED'].tolist(), [None, None, None, None, 0.0, None])
        self.assertEqual(result.loc['w/US Storages'].tolist(), [11124200.0, None, 0.0, 0.0, None, None])

    def test_extract_fcr_text_unavailable_date(self):
        """
        FCR reports are only published for completed days (Pacific), so requests for today or a future
        date raise ValueError without querying WCDS; yesterday is the most recent valid request
        """
        pacific = ZoneInfo('US/Pacific')
        today = dt.datetime.now(tz=pacific).replace(hour=0, minute=0, second=0, microsecond=0)

        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            mock_response.return_value.text = 'Sacramento Valley\nreport text\nSan Joaquin Valley'

            # today (start and end of day) and tomorrow are unavailable
            for requested in (today,
                              today.replace(hour=23, minute=59),
                              today + dt.timedelta(days=1)):
                with self.subTest(requested=requested):
                    with self.assertRaisesRegex(ValueError, f'FCR report unavailable for {requested:%Y-%m-%d}'):
                        wcds.extract_fcr_text(requested)

            # today in Pacific is unavailable even when requested in another timezone
            with self.assertRaises(ValueError):
                wcds.extract_fcr_text(today.replace(hour=12).astimezone(ZoneInfo('UTC')))

            mock_response.assert_not_called()

            # yesterday is available and requests the most recent report (days=0)
            wcds.extract_fcr_text(today - dt.timedelta(days=1))
            mock_response.assert_called_once()
            self.assertIn('days=0&', mock_response.call_args.args[0])

    def test_extract_sac_valley_fcr_data(self):
        """
        Grabs this date of the report:

        Corps of Engineers Flood Control Requirements for California Reservoirs
        Data Ending 2400 hours   01 JAN 2025
        Report Generated 02 JAN 2025 @ 2300


        Assertion should then use: 10.89 and 966823
        """
        result = wcds.extract_sac_valley_fcr_data(dt.datetime(2025, 1, 1, tzinfo=ZoneInfo('US/Pacific')))
        self.assertEqual(float(result.loc['Oroville', 'Flood Control Parameters (Rain in.)']), 10.89)
        self.assertEqual(float(result.loc['Folsom', 'Gross Pool (acft)']), 966823)

    def test_extract_folsom_fcr_data(self):
        # date has No Forecast rows in table
        result = wcds.extract_folsom_fcr_data(dt.datetime(2025, 1, 1, tzinfo=ZoneInfo('US/Pacific')))
        self.assertEqual(float(result.loc['02JAN2025 12z', '3-Day Forecasted Volume']), 27573)
        self.assertEqual(float(result.loc['02JAN2025 18z', 'Top of Conservation (acft)']), 566823)
        # date has 5 forecast dates
        result = wcds.extract_folsom_fcr_data(dt.datetime(2025, 2, 2, tzinfo=ZoneInfo('US/Pacific')))
        self.assertEqual(float(result.loc['03FEB2025 18z', '5-Day Forecasted Volume']), 228664)

    def test_get_fcr_data(self):
        result = wcds.get_fcr_data(dt.datetime(2023, 1, 13, tzinfo=ZoneInfo('US/Pacific')))
        self.assertEqual(float(result['fcr'].loc['Oroville', 'Flood Control Parameters (Rain in.)']), 13.06)
        self.assertEqual(float(result['fcr'].loc['Folsom', 'Gross Pool (acft)']), 966823)
        self.assertEqual(float(result['folsom'].loc['14JAN2023 24z', '2-Day Forecasted Volume']), 123031)
        self.assertEqual(float(result['totals'].loc['BASIN TOTALS', 'Above Top of Conservation (acft)']), -1947118)

    @staticmethod
    def _mock_fcr_report(folsom_rows):
        """
        minimal FCR report text with the given Folsom forecast rows, for offline parser tests
        """
        return '\n'.join([
            '                           ---- Sacramento Valley ----',
            ' Folsom:          966,823                                                 Forecasted Volumes****',
            textwrap.dedent(folsom_rows).strip('\n'),
            ' ____________________________________________________________________________________',
            ' BASIN TOTALS  10,469,900 7,636,111 4,922,524  47     -2,713,587(  0)',
            '                           ---- San Joaquin Valley ----',
        ])

    def _extract_folsom_from_text(self, folsom_rows, **kwargs):
        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            mock_response.return_value.text = self._mock_fcr_report(folsom_rows)
            return wcds.extract_folsom_fcr_data(dt.datetime(2025, 1, 1, tzinfo=ZoneInfo('US/Pacific')), **kwargs)

    def test_extract_folsom_fcr_data_not_reported(self):
        """
        a not-reported (-NR-) forecast row is returned with None values, or `nr_value` when given, rather
        than invalidating the table (report data ending 18 NOV 2026)
        """
        rows = """
          18NOV2026 18              566,823   363,627  38       -203,196(  0)        3,974;   14,980;   23,547;   33,685
         --------------------------------------------------------------------------------------------------------
          19NOV2026 12              566,823   365,307  38       -201,516(  0)        7,454;   14,687;   19,671;   27,968
          19NOV2026 18              566,823      -NR-     -NR-     -NR- (  0)        6,396;   11,952;   16,153;   23,620
          No Forecast
          No Forecast
        """
        result = self._extract_folsom_from_text(rows)
        self.assertEqual(result.index.tolist(), ['18NOV2026 18z', '19NOV2026 12z', '19NOV2026 18z'])
        self.assertEqual(result.loc['19NOV2026 18z'].tolist(),
                         [566823.0, None, None, None, 0.0, 6396.0, 11952.0, 16153.0, 23620.0])
        # values below top of conservation keep their sign
        self.assertEqual(result.loc['18NOV2026 18z', 'Above Top of Conservation(acft)'], -203196.0)

        result = self._extract_folsom_from_text(rows, nr_value='NR')
        self.assertEqual(result.loc['19NOV2026 18z'].tolist(),
                         [566823.0, 'NR', 'NR', 'NR', 0.0, 6396.0, 11952.0, 16153.0, 23620.0])

    def test_extract_folsom_fcr_data_irregular_rows(self):
        """
        negative percent encroached, repeated timestamps, and truncated rows are parsed per row
        (report data ending 21 DEC 2025 and 10 JAN 2026)
        """
        result = self._extract_folsom_from_text("""
          21DEC2025 24              559,464   460,923  48        -98,541(  0)       60,636;  137,712;  193,613;  341,691
         --------------------------------------------------------------------------------------------------------
          21DEC2025 24              559,464   465,844  48        -97,270(  0)       65,781;  106,337;  168,639;  321,008
          22DEC2025 12              566,823         0   0       -566,823(-142)       9,259;   19,029;   28,449;   46,784
          22DEC2025 18              560,378
          No Forecast
        """)
        self.assertEqual(result.index.tolist(), ['21DEC2025 24z', '21DEC2025 24z', '22DEC2025 12z', '22DEC2025 18z'])
        self.assertEqual(result['Actual Res (acft)'].tolist(), [460923.0, 465844.0, 0.0, None])
        self.assertEqual(result.loc['22DEC2025 12z', 'Percent Encroached'], -142.0)
        self.assertEqual(result.loc['22DEC2025 18z'].tolist(), [560378.0] + [None] * 8)

    def test_extract_folsom_fcr_data_none_reported(self):
        """
        a table with no forecast rows returns None (report data ending 22 JAN 2026)
        """
        with mock.patch('builtins.print'):
            result = self._extract_folsom_from_text("""
                                                     -NR-     -NR-   -NR-     (  0)  No Forecast   orecast   orecast   orecast
             --------------------------------------------------------------------------------------------------------
              No Forecast                                 FBO12
              No Forecast
            """)
        self.assertIsNone(result)

    def test_extract_sac_valley_fcr_data_signs_and_not_reported(self):
        """
        values below top of conservation keep their sign, and not-reported (-NR-) and not-applicable (----)
        values are None, and a blank Top of Conservation does not shift later columns (report data ending
        24 JAN 2026; Stony Gorge row from an earlier report)
        """
        report = '\n'.join([
            '                           ---- Sacramento Valley ----',
            ' -------------- --------- --------- --------- -------- -------------- ---------------',
            ' Shasta:        4,552,100 3,516,536 3,655,497  80        138,961( 13) 304146 CFS',
            ' Black Butte:     136,200    39,374    27,189  20        -12,185(  0)  3.30      ---',
            '   East Park:      50,900                -NR-',
            '   Stony Gorge:    50,000              38,311  77',
            ' Oroville:      3,425,000      -NR- 2,780,358  81           -NR-(  0)  -NR-      ---',
            ' New Bullards:    966,000   796,000   802,184  83          6,184(  4)  ----      ---',
            ' Indian Valley:   300,600   260,600   235,700  78        -24,900(  0)  ----      ---',
            ' Folsom:          966,823                                                 Forecasted Volumes****',
            '                           ---- San Joaquin Valley ----',
        ])
        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            mock_response.return_value.text = report
            result = wcds.extract_sac_valley_fcr_data(dt.datetime(2025, 1, 1, tzinfo=ZoneInfo('US/Pacific')))

        above = 'Above top of Conservation (acft)'
        self.assertEqual(result.loc['Indian Valley', above], -24900.0)
        self.assertEqual(result.loc['Black Butte', above], -12185.0)
        self.assertEqual(result.loc['Shasta', above], 138961.0)
        self.assertEqual(result.loc['Oroville'].tolist(), [3425000.0, None, 2780358.0, 81.0, None, 0.0, None, None])
        self.assertEqual(result.loc['East Park'].tolist(), [50900.0] + [None] * 7)
        self.assertEqual(result.loc['Stony Gorge'].tolist(), [50000.0, None, 38311.0, 77.0] + [None] * 4)
        self.assertEqual(result.loc['Folsom', 'Gross Pool (acft)'], 966823.0)

        # not-reported values are distinguishable from not-applicable (----) values when `nr_value` is given
        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            mock_response.return_value.text = report
            result = wcds.extract_sac_valley_fcr_data(dt.datetime(2025, 1, 1, tzinfo=ZoneInfo('US/Pacific')),
                                                      nr_value='NR')
        self.assertEqual(result.loc['Oroville'].tolist(), [3425000.0, 'NR', 2780358.0, 81.0, 'NR', 0.0, 'NR', None])
        self.assertEqual(result.loc['Indian Valley', above], -24900.0)

    def test_get_fcr_data_table_failure(self):
        """
        a table that fails to parse is returned as None without discarding the other tables
        """
        requested = dt.datetime.now(tz=ZoneInfo('US/Pacific')) - dt.timedelta(days=2)
        with mock.patch.object(wcds, 'extract_sac_valley_fcr_data', return_value='fcr table'), \
             mock.patch.object(wcds, 'extract_folsom_fcr_data', side_effect=IndexError('list index out of range')), \
             mock.patch.object(wcds, 'extract_basin_totals', return_value='totals table') as mock_totals, \
             mock.patch('builtins.print') as mock_print:
            result = wcds.get_fcr_data(requested, nr_value='NR')

        self.assertEqual(result, {'date': f'{requested:%Y-%m-%d}',
                                  'fcr': 'fcr table',
                                  'folsom': None,
                                  'totals': 'totals table'})
        self.assertIn('FCR `folsom` table could not be parsed', mock_print.call_args.args[0])
        mock_totals.assert_called_once_with(requested, nr_value='NR')

    def test_get_fcr_data_unavailable_date(self):
        """
        an unavailable date raises ValueError rather than returning a result with every table None
        """
        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            with self.assertRaises(ValueError):
                wcds.get_fcr_data(dt.datetime.now(tz=ZoneInfo('US/Pacific')))
            mock_response.assert_not_called()

if __name__ == '__main__':
    unittest.main()
