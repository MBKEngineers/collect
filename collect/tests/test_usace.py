"""
collect.tests.test_usace
============================================================
initial test suite for collect.usace data access and utility functions; note: these tests require internet connection
"""
import datetime as dt
import textwrap
import unittest
from functools import partial
from unittest import mock
from zoneinfo import ZoneInfo
from collect.usace import wcds


def _with_fcr_header(requested, content):
    """
    FCR report text with the "Data Ending" header for the requested date (Pacific) before `content`, for tests that
    mock the report fetch
    """
    date = requested.astimezone(ZoneInfo('US/Pacific'))
    month = f'{date:%b}'.upper()
    return f' Data Ending 2400 hours   {date:%d} {month} {date:%Y}\n{content}'


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
        result = wcds.extract_basin_totals(dt.datetime(2025, 1, 1, tzinfo=ZoneInfo('US/Pacific')),
                                           basin='Sacramento Valley')
        self.assertEqual(float(result.loc['BASIN TOTALS', 'Percent Encroached']), 5.0)
        # earlier format without percent encroached or the total flood space encroached row
        result = wcds.extract_basin_totals(dt.datetime(2015, 1, 1, tzinfo=ZoneInfo('US/Pacific')),
                                           basin='Sacramento Valley')
        self.assertEqual(result.loc['BASIN TOTALS'].tolist(),
                         [10469900.0, 7919960.0, 4238480.0, 40.0, -3681480.0, None])
        self.assertEqual(result.loc['w/US Storages'].tolist(), [11124200.0, None, 4563619.0, 41.0, None, None])

    def _extract_basin_totals_from_text(self, totals_rows, **kwargs):
        report = '\n'.join([
            '                           ---- Sacramento Valley ----',
            ' -------------- --------- --------- --------- -------- -------------- ---------------',
            *totals_rows,
            '',
            ' **  Percent Encroached',
            '                           ---- San Joaquin Valley ----',
        ])
        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            requested = dt.datetime(2025, 1, 1, tzinfo=ZoneInfo('US/Pacific'))
            mock_response.return_value.text = _with_fcr_header(requested, report)
            return wcds.extract_basin_totals(requested, basin='Sacramento Valley', **kwargs)

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

        # columns without missing or not-reported values are float; others keep None and `nr_value`
        self.assertEqual(result['Gross Pool (acft)'].dtype, float)
        self.assertEqual(result['Actual Res (acft)'].dtype, object)

    def _extract_basin_totals_from_lines(self, section_lines, basin, requested, **kwargs):
        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            mock_response.return_value.text = _with_fcr_header(requested, '\n'.join(section_lines))
            return wcds.extract_basin_totals(requested, basin=basin, **kwargs)

    def test_extract_basin_totals_by_basin_blank_columns(self):
        """
        basin totals are read for other basin sections, with values assigned by column position so blank
        columns stay in place; Russian River reports no w/US Storages row (report data ending 22 JAN 2026)
        """
        lines = [
            '                          ---- Russian River Basin----',
            '                -------------------- Storages -----------------------  Flood Control',
            '                   Gross    Top of     Actual  % of       Above Top     Parameters',
            '                   Pool  Conservation   Res    Gross     of Conserv    Rain     Snow',
            '                   (acft) (acft)***    (acft)  Pool       (acft)( **)  (in)    (acft)',
            ' -------------- --------- --------- --------- -------- -------------- ---------------',
            ' BASIN TOTALS     497,500      -NR-                         -NR-(  0)',
            ' TOTAL FLOOD SPACE ENCROACHED                                 0',
            '',
            ' **  Percent Encroached',
        ]
        result = self._extract_basin_totals_from_lines(lines, 'Russian River Basin',
                                                       dt.datetime(2026, 1, 22, tzinfo=ZoneInfo('US/Pacific')),
                                                       nr_value='NR')
        self.assertEqual(result.index.tolist(), ['BASIN TOTALS', 'TOTAL FLOOD SPACE ENCROACHED'])
        self.assertEqual(result.loc['BASIN TOTALS'].tolist(), [497500.0, 'NR', None, None, 'NR', 0.0])
        self.assertEqual(result.loc['TOTAL FLOOD SPACE ENCROACHED'].tolist(), [None, None, None, None, 0.0, None])

    def test_extract_basin_totals_values_without_spaces(self):
        """
        values printed without a space between them keep their own columns: an unfilled placeholder after a
        number, and two numbers (report data ending 27 MAY 2014)
        """
        requested = dt.datetime(2014, 5, 27, tzinfo=ZoneInfo('US/Pacific'))
        result = self._extract_basin_totals_from_lines([
            '                           ---- San Joaquin Valley ----',
            '                -------------------- Storages -----------------------  Flood Control',
            '                   Gross    Top of     Actual  % of       Above Top     Parameters',
            '                   Pool  Conservation   Res    Gross     of Conserv    Rain     Snow',
            '                   (acft) (acft)***    (acft)  Pool         (acft)     (in)    (acft)',
            ' -------------- --------- --------- --------- -------- -------------- ---------------',
            ' BASIN TOTALS   7,186,300%BBSTCS    2,857,502  40     %BBSSP     %BBS2',
            ' w/US Storages  9,018,450           4,169,585  46',
        ], 'San Joaquin Valley', requested)
        self.assertEqual(result.loc['BASIN TOTALS'].tolist(), [7186300.0, None, 2857502.0, 40.0, None, None])
        self.assertEqual(result.loc['w/US Storages'].tolist(), [9018450.0, None, 4169585.0, 46.0, None, None])

        result = self._extract_basin_totals_from_lines([
            '                           ---- Sacramento Valley ----',
            '                -------------------- Storages -----------------------  Flood Control',
            '                   Gross    Top of     Actual  % of       Above Top     Parameters',
            '                   Pool  Conservation   Res    Gross     of Conserv    Rain     Snow',
            '                   (acft) (acft)***    (acft)  Pool         (acft)     (in)    (acft)',
            ' -------------- --------- --------- --------- -------- -------------- ---------------',
            ' BASIN TOTALS  10,469,90010,439,068 5,294,714  51     -5,144,354          0',
            ' w/US Storages 11,124,200           5,531,111  50',
        ], 'Sacramento Valley', requested)
        self.assertEqual(result.loc['BASIN TOTALS'].tolist(),
                         [10469900.0, 10439068.0, 5294714.0, 51.0, -5144354.0, None])

    def test_extract_basin_totals_none_reported_for_basin(self):
        """
        a basin section without basin totals returns None without a warning (So. San Francisco Bay Basin)
        """
        with mock.patch('builtins.print') as mock_print:
            result = self._extract_basin_totals_from_lines([
                '                       ---- So. San Francisco Bay Basin ----',
                ' -------------- --------- --------- --------- -------- -------------- ---------------',
                ' Del Valle:        77,000    39,000    27,797  36(  0)   -11,203(  0)  ----      ---',
                ' ____________________________________________________________________________________',
            ], 'So. San Francisco Bay Basin', dt.datetime(2026, 11, 19, tzinfo=ZoneInfo('US/Pacific')))
        self.assertIsNone(result)
        mock_print.assert_not_called()

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

    def test_get_fcr_report_unavailable_date(self):
        """
        FCR reports are only published for completed days (Pacific), so requests for today or a future
        date raise ValueError without querying WCDS; yesterday is the most recent valid request
        """
        pacific = ZoneInfo('US/Pacific')
        today = dt.datetime.now(tz=pacific).replace(hour=0, minute=0, second=0, microsecond=0)

        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            mock_response.return_value.text = _with_fcr_header(today - dt.timedelta(days=1),
                                                               'Sacramento Valley\nreport text\nSan Joaquin Valley')

            # today (start and end of day) and tomorrow are unavailable
            for requested in (today,
                              today.replace(hour=23, minute=59),
                              today + dt.timedelta(days=1)):
                with self.subTest(requested=requested):
                    with self.assertRaisesRegex(ValueError, f'FCR report unavailable for {requested:%Y-%m-%d}'):
                        wcds._get_fcr_report(requested)

            # today in Pacific is unavailable even when requested in another timezone
            with self.assertRaises(ValueError):
                wcds._get_fcr_report(today.replace(hour=12).astimezone(ZoneInfo('UTC')))

            mock_response.assert_not_called()

            # yesterday is available and requests the most recent report (days=0)
            wcds._get_fcr_report(today - dt.timedelta(days=1))
            mock_response.assert_called_once()
            self.assertIn('days=0&', mock_response.call_args.args[0])

    def test_get_fcr_report_date_mismatch(self):
        """
        a fetched report whose data ending date is not the requested date (Pacific), e.g. the previous day's report
        before yesterday's is published, or a response without the header, raises ValueError rather than returning
        another day's data
        """
        pacific = ZoneInfo('US/Pacific')
        today = dt.datetime.now(tz=pacific).replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = today - dt.timedelta(days=1)

        for content, returned in ((_with_fcr_header(yesterday - dt.timedelta(days=1), 'report text'),
                                   f'data ending {yesterday - dt.timedelta(days=1):%Y-%m-%d}'),
                                  ('report text', 'no data ending date')):
            with (self.subTest(returned=returned),
                  mock.patch.object(wcds.utils, 'get_session_response') as mock_response):
                mock_response.return_value.text = content
                for extract in (wcds._get_fcr_report, wcds.get_fcr_data):
                    with self.assertRaisesRegex(ValueError, f'FCR report unavailable for {yesterday:%Y-%m-%d}; '
                                                            f'WCDS returned a report with {returned}'):
                        extract(yesterday)

        # the report date is the Pacific date, also when requested in another timezone
        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            mock_response.return_value.text = _with_fcr_header(yesterday, 'report text')
            self.assertEqual(wcds._get_fcr_report(yesterday.replace(hour=23).astimezone(ZoneInfo('UTC'))),
                             mock_response.return_value.text)

    def test_get_fcr_data_date_pacific(self):
        """
        the returned date is the report date (Pacific), also when the request is in another timezone on a different
        calendar day
        """
        pacific = ZoneInfo('US/Pacific')
        today = dt.datetime.now(tz=pacific).replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = today - dt.timedelta(days=1)
        # 18:00 Pacific is the next calendar day in UTC
        requested = yesterday.replace(hour=18).astimezone(ZoneInfo('UTC'))
        self.assertNotEqual(requested.date(), yesterday.date())

        with (mock.patch.object(wcds.utils, 'get_session_response') as mock_response,
              mock.patch('builtins.print')):
            month = f'{today:%b}'.upper()
            mock_response.return_value.text = _with_fcr_header(
                yesterday, f' Report Generated {today:%d} {month} {today:%Y} @ 1200')
            result = wcds.get_fcr_data(requested)
        self.assertEqual(result['date'], f'{yesterday:%Y-%m-%d}')
        self.assertEqual(result['generated'], today.replace(hour=12))

    def test_get_fcr_report_generated(self):
        """
        the generated time is read from the report header as a US/Pacific datetime, including single-digit days
        """
        pacific = ZoneInfo('US/Pacific')
        self.assertEqual(wcds._get_fcr_report_generated(' Report Generated 23 NOV 2026 @ 1200    \n'),
                         dt.datetime(2026, 11, 23, 12, 0, tzinfo=pacific))
        self.assertEqual(wcds._get_fcr_report_generated(' Report Generated  7 FEB 2014 @ 1121'),
                         dt.datetime(2014, 2, 7, 11, 21, tzinfo=pacific))
        self.assertIsNone(wcds._get_fcr_report_generated(' Data Ending 2400 hours   22 NOV 2026'))

    def test_get_fcr_report_date(self):
        """
        the data ending date is read from the report header, including single-digit days
        """
        self.assertEqual(wcds._get_fcr_report_date(' Data Ending 2400 hours   22 NOV 2026        \n'),
                         dt.date(2026, 11, 22))
        self.assertEqual(wcds._get_fcr_report_date(' Data Ending 2400 hours    7 FEB 2014'), dt.date(2014, 2, 7))
        self.assertIsNone(wcds._get_fcr_report_date('Report Generated 23 NOV 2026 @ 1200'))

    def test_extract_basin_fcr_table_sacramento_valley(self):
        """
        Grabs this date of the report:

        Corps of Engineers Flood Control Requirements for California Reservoirs
        Data Ending 2400 hours   01 JAN 2025
        Report Generated 02 JAN 2025 @ 2300


        Assertion should then use: 10.89 and 966823
        """
        result = wcds.extract_basin_fcr_table(dt.datetime(2025, 1, 1, tzinfo=ZoneInfo('US/Pacific')), 'Sacramento Valley')
        self.assertEqual(float(result.loc['Oroville', 'Flood Control Parameters (Rain in.)']), 10.89)
        self.assertEqual(float(result.loc['Folsom', 'Gross Pool (acft)']), 966823)

    def test_get_fcr_folsom_forecast(self):
        # date has No Forecast rows in table
        result = wcds._get_fcr_folsom_forecast(dt.datetime(2025, 1, 1, tzinfo=ZoneInfo('US/Pacific')))
        self.assertEqual(float(result.loc['02JAN2025 12z', '3-Day Forecasted Volume']), 27573)
        self.assertEqual(float(result.loc['02JAN2025 18z', 'Top of Conservation (acft)']), 566823)
        # date has 5 forecast dates
        result = wcds._get_fcr_folsom_forecast(dt.datetime(2025, 2, 2, tzinfo=ZoneInfo('US/Pacific')))
        self.assertEqual(float(result.loc['03FEB2025 18z', '5-Day Forecasted Volume']), 228664)

    def test_get_fcr_data(self):
        result = wcds.get_fcr_data(dt.datetime(2023, 1, 13, tzinfo=ZoneInfo('US/Pacific')))
        self.assertEqual(list(result), ['date', 'generated', *wcds.FCR_BASINS])
        self.assertEqual(result['generated'], dt.datetime(2023, 1, 18, 6, 57, tzinfo=ZoneInfo('US/Pacific')))
        sacramento = result['Sacramento Valley']
        self.assertEqual(float(sacramento['fcr'].loc['Oroville', 'Flood Control Parameters (Rain in.)']), 13.06)
        self.assertEqual(float(sacramento['fcr'].loc['Folsom', 'Gross Pool (acft)']), 966823)
        self.assertEqual(float(sacramento['folsom'].loc['14JAN2023 24z', '2-Day Forecasted Volume']), 123031)
        self.assertEqual(float(sacramento['totals'].loc['BASIN TOTALS', 'Above Top of Conservation (acft)']), -1947118)
        self.assertEqual(list(sacramento), ['fcr', 'totals', 'footnotes', 'folsom'])
        self.assertTrue(sacramento['footnotes'].startswith(' **  Percent Encroached'))
        self.assertEqual(list(result['Tulare Lake Basin']), ['fcr', 'totals', 'footnotes'])

    @staticmethod
    def _mock_fcr_report(folsom_rows):
        """
        minimal FCR report text with the given Folsom forecast rows, for offline parser tests
        """
        return '\n'.join([
            '                           ---- Sacramento Valley ----',
            ' -------------- --------- --------- --------- -------- -------------- ---------------',
            ' Folsom:          966,823                                                 Forecasted Volumes****',
            textwrap.dedent(folsom_rows).strip('\n'),
            ' ____________________________________________________________________________________',
            ' BASIN TOTALS  10,469,900 7,636,111 4,922,524  47     -2,713,587(  0)',
            '                           ---- San Joaquin Valley ----',
        ])

    def _extract_folsom_from_text(self, folsom_rows, **kwargs):
        return wcds._parse_fcr_folsom_forecast(self._mock_fcr_report(folsom_rows), **kwargs)

    def test_parse_fcr_folsom_forecast_not_reported(self):
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

        # columns without missing or not-reported values are float; others keep None and `nr_value`
        self.assertEqual(result['Top of Conservation (acft)'].dtype, float)
        self.assertEqual(result['3-Day Forecasted Volume'].dtype, float)
        self.assertEqual(result['Actual Res (acft)'].dtype, object)

    def test_parse_fcr_folsom_forecast_irregular_rows(self):
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

    def test_parse_fcr_folsom_forecast_date_only_row(self):
        """
        a row with only a forecast date and time is kept with all values None rather than dropped
        """
        result = self._extract_folsom_from_text("""
          22DEC2025 12              566,823   363,627  38       -203,196(  0)        3,974;   14,980;   23,547;   33,685
         --------------------------------------------------------------------------------------------------------
          22DEC2025 18
          No Forecast
        """)
        self.assertEqual(result.index.tolist(), ['22DEC2025 12z', '22DEC2025 18z'])
        self.assertEqual(result.loc['22DEC2025 18z'].tolist(), [None] * 9)

    def test_parse_fcr_folsom_forecast_none_reported(self):
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

    def test_parse_fcr_folsom_forecast_unplaced_values(self):
        """
        storages are assigned by position, so forecast volumes without the parenthesized percent encroached are still
        told apart from the storages; two values in one storage column, or more forecast volumes than columns, warn;
        a truncated row with storages only does not warn
        """
        with mock.patch('builtins.print') as mock_print:
            result = self._extract_folsom_from_text("""
              19NOV2026 12              566,823   365,307  38       -201,516           7,454;   14,687;   19,671;   27,968
              19NOV2026 18              566,823   365,307  38       -201,516   12(  0)  7,454;   14,687;   19,671;   27,968
              20NOV2026 12              566,823   365,307  38       -201,516(  0)      7,454;   14,687;   19,671;   27,968;  1
              22DEC2025 18              560,378
            """)
        warnings = [call.args[0] for call in mock_print.call_args_list]
        self.assertEqual(len(warnings), 2)
        self.assertIn("values '-201,516' and '12' are in the same column; '-201,516' was ignored", warnings[0])
        self.assertIn('more forecast volumes than columns', warnings[1])
        self.assertEqual(result.loc['19NOV2026 12z'].tolist(),
                         [566823.0, 365307.0, 38.0, -201516.0, None, 7454.0, 14687.0, 19671.0, 27968.0])
        self.assertEqual(result.loc['20NOV2026 12z'].tolist(),
                         [566823.0, 365307.0, 38.0, -201516.0, 0.0, 7454.0, 14687.0, 19671.0, 27968.0])
        self.assertEqual(result.loc['22DEC2025 18z'].tolist(), [560378.0] + [None] * 8)

    def test_parse_fcr_folsom_forecast_by_position(self):
        """
        storages are assigned to columns by the positions of the Sacramento Valley rule line in reports from the first
        with the table (data ending 02 JUL 2019, forecast volumes two characters further left) to recent reports (data
        ending 26 FEB 2024), and a blank storage does not shift later values
        """
        with mock.patch('builtins.print') as mock_print:
            result = self._extract_folsom_from_text("""
              02JUL2019 12              966,823   916,248  95        -50,575(  0)      6,344;   13,547;   20,468;   33,816
              26FEB2024 18              566,823   614,726  64         47,903( 12)        9,437;   18,501;   26,926;   83,325
             --------------------------------------------------------------------------------------------------------
              27FEB2024 12              566,823   613,116  63         46,293( 12)        9,264;   18,387;   28,759;  114,311
              27FEB2024 18              566,823                       46,115( 12)        9,402;   19,156;   32,458;  119,598
              No Forecast
            """)
        mock_print.assert_not_called()
        self.assertEqual(result.loc['02JUL2019 12z'].tolist(),
                         [966823.0, 916248.0, 95.0, -50575.0, 0.0, 6344.0, 13547.0, 20468.0, 33816.0])
        self.assertEqual(result.loc['27FEB2024 12z'].tolist(),
                         [566823.0, 613116.0, 63.0, 46293.0, 12.0, 9264.0, 18387.0, 28759.0, 114311.0])
        # blank actual and % of gross pool stay blank rather than taking the above top of conservation value
        self.assertEqual(result.loc['27FEB2024 18z'].tolist(),
                         [566823.0, None, None, 46115.0, 12.0, 9402.0, 19156.0, 32458.0, 119598.0])

    def test_parse_fcr_basin_table_unplaced_values(self):
        """
        values that cannot be assigned to a column warn rather than being placed silently in another column: a
        non-numeric parenthesized percent encroached is read whole as the percent encroached, two values in one column,
        a value beyond the last column, and a row with values but no recognized reservoir label
        """
        rule = ' -------------- --------- --------- --------- -------- -------------- ---------------'
        table = partial(wcds._parse_fcr_basin_table, nr_value='NR')

        # a parenthesized -NR- is the percent encroached, not a value in the rain column
        with mock.patch('builtins.print') as mock_print:
            result = table('\n'.join([rule,
                                      ' Indian Valley:   300,600   260,600   235,700  78        -24,900(-NR-)  0.12      ---',
                                      ' Black Butte:     136,200    69,800    13,560  10        -56,240( N/A)']))
        mock_print.assert_not_called()
        self.assertEqual(result.loc['Indian Valley'].tolist(), [300600.0, 260600.0, 235700.0, 78.0, -24900.0, 'NR', 0.12, None])
        self.assertEqual(result.loc['Black Butte'].tolist(), [136200.0, 69800.0, 13560.0, 10.0, -56240.0, None, None, None])

        with mock.patch('builtins.print') as mock_print:
            result = table('\n'.join([rule,
                                      ' Indian Valley:   300,600 1 260,600   235,700  78        -24,900(  0)  0.12      ---',
                                      ' Black Butte:     136,200    69,800    13,560  10        -56,240(  0)  0.28      ---  7',
                                      ' Folsom (COE):    977,000   577,000   162,652  17       -414,348(  0)  0.91      ---']))
        warnings = [call.args[0] for call in mock_print.call_args_list]
        self.assertEqual(len(warnings), 3)
        self.assertIn("values '1' and '260,600' are in the same column; '1' was ignored", warnings[0])
        self.assertIn("value '7' is beyond the last column and was ignored", warnings[1])
        self.assertIn("row not recognized as a reservoir and was ignored: 'Folsom (COE):", warnings[2])
        self.assertEqual(result.index.tolist(), ['Indian Valley', 'Black Butte'])

    def test_parse_fcr_basin_table_empty(self):
        """
        a table without reservoir rows with values (no rows, or only group label rows) returns None with a warning
        rather than an empty DataFrame
        """
        rule = ' -------------- --------- --------- --------- -------- -------------- ---------------'
        for section in ([rule],
                        [rule, ' EBMUD Reservoirs:', ' ______________'],
                        [rule, ' Folsom (COE):    977,000   577,000   162,652  17       -414,348(  0)  0.91      ---']):
            with self.subTest(section=section), mock.patch('builtins.print') as mock_print:
                self.assertIsNone(wcds._parse_fcr_basin_table('\n'.join(section)))
            self.assertIn('not reported or format not supported', mock_print.call_args_list[-1].args[0])

    def test_parse_fcr_basin_table_underscores_above_headers(self):
        """
        a line of underscores above the column headers does not end the table; the table ends at the line of
        underscores after the rule line
        """
        lines = [
            ' ____________________________________________________________________________________',
            '                Gross Pool  Top of    Actual',
            ' -------------- --------- --------- --------- -------- -------------- ---------------',
            ' Boca:             41,100    40,000    20,000  49        -20,000(  0)  ----       N/A',
            ' ____________________________________________________________________________________',
            ' BASIN TOTALS     317,800         0         0   0              0(  0)',
        ]
        with mock.patch('builtins.print') as mock_print:
            result = wcds._parse_fcr_basin_table('\n'.join(lines))
        mock_print.assert_not_called()
        self.assertEqual(result.index.tolist(), ['Boca'])

    def test_extract_basin_fcr_table_sacramento_valley_safca(self):
        """
        before the Folsom forecast volumes table (mid-2019), the Sacramento Valley table continues past the Folsom row
        with the Folsom w/SAFCA FC Diagram row, whose acre-feet flood control parameter is read in the rain column like
        Shasta's CFS, and the upstream reservoirs (report data ending 28 JUL 2014; SAFCA row from 23 APR 2017)
        """
        report = '\n'.join([
            ' Corps of Engineers Flood Control Requirements for California Reservoirs',
            ' Data Ending 2400 hours   28 JUL 2014',
            ' Report Generated  8 SEP 2014 @ 0810',
            ' ',
            '                           ---- Sacramento Valley ----',
            '                -------------------- Storages -----------------------  Flood Control',
            '                   Gross    Top of     Actual  % of       Above Top     Parameters',
            '                   Pool  Conservation   Res    Gross     of Conserv    Rain     Snow',
            '                   (acft) (acft)***    (acft)  Pool         (acft)     (in)    (acft)',
            ' -------------- --------- --------- --------- -------- -------------- ---------------',
            ' Shasta:        4,552,100 4,552,100 1,600,122  35     -2,951,978(100)  51580 CFS',
            ' Black Butte:     136,200   136,200    57,008  42        -79,192(100)  0.38      ---',
            '   East Park:      50,900               8,306  16',
            '   Stony Gorge:    50,000               8,647  17',
            ' Oroville:      3,538,000 3,538,000 1,268,653  36     -2,269,347(100)  0.41      ---',
            ' New Bullards:    966,000   966,000   522,170  54       -443,830(100)  ----      ---',
            ' Indian Valley:   300,600   300,600    25,900   9       -274,700(100)  ----      ---',
            '*Folsom:          977,000   977,000   410,805  42       -566,195(100)  0.57      ---',
            ' Folsom w/SAFCA FC Diagram: 977,000   410,805  42       -566,195(100) 166450 ACFT',
            '   Union Valley:  235,100             193,650  82',
            '   French Mdw:    110,700              60,379  55',
            '   Hell Hole:     207,600             103,391  50',
            ' ____________________________________________________________________________________',
            ' BASIN TOTALS  10,469,90010,469,900 3,884,658  37     -6,585,242          0',
            ' w/US Storages 11,124,200           4,259,031  38',
            ' ',
            ' *   Folsom (COE) Diagram not used in Calculations',
        ])
        requested = dt.datetime(2014, 7, 28, tzinfo=ZoneInfo('US/Pacific'))
        with mock.patch('builtins.print') as mock_print:
            result = wcds.extract_basin_fcr_table(requested, 'Sacramento Valley', report=report)
        mock_print.assert_not_called()
        self.assertEqual(result.index.tolist(), ['Shasta', 'Black Butte', 'East Park', 'Stony Gorge', 'Oroville',
                                                 'New Bullards', 'Indian Valley', 'Folsom', 'Folsom w/SAFCA FC Diagram',
                                                 'Union Valley', 'French Mdw', 'Hell Hole'])
        # the report does not label the parenthesized values as percent encroached, so % Encroached is None
        self.assertEqual(result.loc['Folsom w/SAFCA FC Diagram'].tolist(),
                         [None, 977000.0, 410805.0, 42.0, -566195.0, None, 166450.0, None])
        self.assertEqual(result.loc['Folsom'].tolist(),
                         [977000.0, 977000.0, 410805.0, 42.0, -566195.0, None, 0.57, None])
        self.assertEqual(result.loc['Hell Hole'].tolist(), [207600.0, None, 103391.0, 50.0, None, None, None, None])

        # a negative SAFCA flood control parameter (report data ending 23 APR 2017)
        safca_2017 = ' Folsom w/SAFCA FC Diagram: 763,250   754,955  77         -8,295      -41965 ACFT'
        with mock.patch('builtins.print') as mock_print:
            result = wcds.extract_basin_fcr_table(requested, 'Sacramento Valley', report='\n'.join(
                [line if not line.startswith(' Folsom w/SAFCA') else safca_2017 for line in report.splitlines()]))
        mock_print.assert_not_called()
        self.assertEqual(result.loc['Folsom w/SAFCA FC Diagram'].tolist(),
                         [None, 763250.0, 754955.0, 77.0, -8295.0, None, -41965.0, None])

    def test_extract_basin_footnotes(self):
        """
        the footnotes are returned unparsed, from the first footnote to the end of the section with continuation lines
        and without trailing blank lines; a marked reservoir row (e.g. "*Folsom:") and the column header markers are
        not footnotes; a section without footnotes returns None (report data ending 18 JAN 2020 and 23 APR 2017)
        """
        requested = dt.datetime(2020, 1, 18, tzinfo=ZoneInfo('US/Pacific'))
        report = '\n'.join([
            '                           ---- Sacramento Valley ----',
            '                   (acft) (acft)***    (acft)  Pool       (acft)( **)  (in)    (acft)',
            ' -------------- --------- --------- --------- -------- -------------- ---------------',
            '*Folsom:          977,000   762,916   754,955  77         -7,960(  0) 13.04      ---',
            ' ____________________________________________________________________________________',
            ' BASIN TOTALS  10,469,900 7,851,647 6,850,433  65     -1,001,214(  0)',
            ' w/US Storages 11,124,200           7,205,448  65',
            ' ',
            ' *   Folsom (COE) Diagram not used in Calculations',
            ' **  Percent Encroached',
            ' *** For a definition of Top of Conservation Storage and other relevant terms,',
            '     see the Glossary of Key Phrases page on the',
            ' **** In order (left to right) the 1 Day; 2 Day; 3 Day; and 5 Day forecasted volumes',
            '      based on the ensemble forecasts provided by cnrfc.noaa.gov',
            '     USACE Sacramento District Water Control Data System web site',
            ' ',
            '',
            '                           ---- Tulare Lake Basin ----',
            ' -------------- --------- --------- --------- -------- -------------- ---------------',
            ' Success:          82,300    82,300    37,870  46        -44,430(  0)  ----       N/A',
            ' ____________________________________________________________________________________',
            ' BASIN TOTALS   1,793,400   866,394   679,427  38       -186,967(  0)',
        ])
        with mock.patch('builtins.print') as mock_print:
            sacramento = wcds.extract_basin_footnotes(requested, 'Sacramento Valley', report=report)
            tulare = wcds.extract_basin_footnotes(requested, 'Tulare Lake Basin', report=report)
        mock_print.assert_not_called()
        self.assertEqual(sacramento, '\n'.join([
            ' *   Folsom (COE) Diagram not used in Calculations',
            ' **  Percent Encroached',
            ' *** For a definition of Top of Conservation Storage and other relevant terms,',
            '     see the Glossary of Key Phrases page on the',
            ' **** In order (left to right) the 1 Day; 2 Day; 3 Day; and 5 Day forecasted volumes',
            '      based on the ensemble forecasts provided by cnrfc.noaa.gov',
            '     USACE Sacramento District Water Control Data System web site',
        ]))
        self.assertIsNone(tulare)

    def test_get_fcr_section_misspelled_heading(self):
        """
        the Truckee River Basin section is found under its misspelled heading "Truckee Rivee Basin" (report data ending
        23 APR 2017), and other basins' headings still match their name exactly
        """
        report = '\n'.join([
            '                           ---- Tulare Lake Basin ----',
            ' Success:          82,300    82,300    37,870  46        -44,430(  0)  ----       N/A',
            '                          ---- Truckee Rivee Basin ----',
            ' Boca:             41,100    40,000    20,000  49        -20,000(  0)  ----       N/A',
            '                          ---- Russian River Basin----',
            ' Coyote:         122,400    70,000    81,900  67         11,900( 23)  ----      ---',
        ])
        truckee = wcds._get_fcr_section(report, 'Truckee River Basin')
        self.assertIn('Boca:', truckee)
        self.assertNotIn('Coyote:', truckee)
        self.assertIn('Coyote:', wcds._get_fcr_section(report, 'Russian River Basin'))
        self.assertIn('Boca:', wcds._get_fcr_section(report.replace('Rivee', 'River'), 'Truckee River Basin'))
        self.assertIsNone(wcds._get_fcr_section(report.replace('Rivee', 'Rive'), 'Truckee River Basin'))

    def test_extract_basin_totals_missing_warns(self):
        """
        missing basin totals warn for basins that report totals (unlike So. San Francisco Bay Basin)
        """
        with mock.patch('builtins.print') as mock_print:
            result = self._extract_basin_totals_from_lines([
                '                           ---- Russian River Basin ----',
                ' -------------- --------- --------- --------- -------- -------------- ---------------',
                ' Coyote:         122,400    70,000    81,900  67         11,900( 23)  ----      ---',
                ' ____________________________________________________________________________________',
            ], 'Russian River Basin', dt.datetime(2026, 11, 19, tzinfo=ZoneInfo('US/Pacific')))
        self.assertIsNone(result)
        mock_print.assert_called_once_with('WARNING: Russian River Basin basin totals not found in FCR report')

    def test_parse_fcr_basin_totals_values_beyond_storages(self):
        """
        values beyond the storage columns of the basin totals (e.g. in 2014 reports) are ignored without a warning
        """
        rule = ' -------------- --------- --------- --------- -------- -------------- ---------------'
        with mock.patch('builtins.print') as mock_print:
            result = wcds._parse_fcr_basin_totals('\n'.join([
                rule, ' BASIN TOTALS  10,469,900 7,636,111 4,922,524  47     -2,713,587(  0)  1.23']))
        mock_print.assert_not_called()
        self.assertEqual(result.loc['BASIN TOTALS'].tolist(), [10469900.0, 7636111.0, 4922524.0, 47.0, -2713587.0, 0.0])

    def test_extract_sac_valley_fcr_data_deprecated(self):
        """
        extract_sac_valley_fcr_data warns that it is deprecated, at the caller's line, and returns the same table as
        extract_basin_fcr_table
        """
        requested = dt.datetime(2026, 11, 19, tzinfo=ZoneInfo('US/Pacific'))
        report = '\n'.join([
            '                           ---- Sacramento Valley ----',
            ' -------------- --------- --------- --------- -------- -------------- ---------------',
            ' Oroville:      3,425,000 2,984,642 1,444,143  42     -1,540,499(  0)  4.81      ---',
            ' Folsom:          966,823                                                 Forecasted Volumes****',
            '',
            ' **  Percent Encroached',
        ])
        with self.assertWarnsRegex(DeprecationWarning, 'extract_sac_valley_fcr_data is deprecated') as warning:
            result = wcds.extract_sac_valley_fcr_data(requested, nr_value='NR', report=report)
        self.assertEqual(warning.filename, __file__)
        expected = wcds.extract_basin_fcr_table(requested, 'Sacramento Valley', nr_value='NR', report=report)
        self.assertTrue(result.equals(expected))

    def test_extract_folsom_fcr_data_deprecated(self):
        """
        extract_folsom_fcr_data warns that it is deprecated, at the caller's line, and returns the same table as
        get_fcr_data(...)['Sacramento Valley']['folsom']
        """
        requested = dt.datetime(2026, 11, 18, tzinfo=ZoneInfo('US/Pacific'))
        report = self._mock_fcr_report("""
          19NOV2026 12              566,823   365,307  38       -201,516(  0)        7,454;   14,687;   19,671;   27,968
          19NOV2026 18              566,823      -NR-     -NR-     -NR- (  0)        6,396;   11,952;   16,153;   23,620
        """)
        with self.assertWarnsRegex(DeprecationWarning, 'extract_folsom_fcr_data is deprecated') as warning:
            result = wcds.extract_folsom_fcr_data(requested, nr_value='NR', report=report)
        self.assertEqual(warning.filename, __file__)
        self.assertTrue(result.equals(wcds._get_fcr_folsom_forecast(requested, nr_value='NR', report=report)))
        self.assertEqual(result.loc['19NOV2026 18z', 'Actual Res (acft)'], 'NR')

    def test_extract_basin_fcr_table_sacramento_valley_signs_and_not_reported(self):
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
            '',
            ' **  Percent Encroached',
            '                           ---- San Joaquin Valley ----',
        ])
        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            mock_response.return_value.text = _with_fcr_header(dt.datetime(2025, 1, 1, tzinfo=ZoneInfo('US/Pacific')), report)
            result = wcds.extract_basin_fcr_table(dt.datetime(2025, 1, 1, tzinfo=ZoneInfo('US/Pacific')), 'Sacramento Valley')

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
            mock_response.return_value.text = _with_fcr_header(dt.datetime(2025, 1, 1, tzinfo=ZoneInfo('US/Pacific')), report)
            result = wcds.extract_basin_fcr_table(dt.datetime(2025, 1, 1, tzinfo=ZoneInfo('US/Pacific')), 'Sacramento Valley',
                                                      nr_value='NR')
        self.assertEqual(result.loc['Oroville'].tolist(), [3425000.0, 'NR', 2780358.0, 81.0, 'NR', 0.0, 'NR', None])
        self.assertEqual(result.loc['Indian Valley', above], -24900.0)

        # columns without missing or not-reported values are still float when other columns contain `nr_value`
        self.assertEqual(result['Gross Pool (acft)'].dtype, float)
        self.assertEqual(result['Top of Conservation (acft)'].dtype, object)

    def test_extract_basin_fcr_table_san_joaquin_valley(self):
        """
        San Joaquin Valley reservoirs, including upstream reservoirs with a blank Top of Conservation, with
        not-reported (-NR-), not-applicable (----, N/A), zero, and negative values (report data ending
        02 OCT 2025)
        """
        report = '\n'.join([
            '                           ---- San Joaquin Valley ----',
            '                -------------------- Storages -----------------------  Flood Control',
            '                   Gross    Top of     Actual  % of       Above Top     Parameters',
            '                   Pool  Conservation   Res    Gross     of Conserv    Rain     Snow',
            '                   (acft) (acft)***    (acft)  Pool       (acft)( **)  (in)    (acft)',
            ' -------------- --------- --------- --------- -------- -------------- ---------------',
            ' EBMUD Reservoirs:',
            '   Camanche:      417,120      -NR-   308,550  74           -NR-(  0)  ----       N/A',
            '   Pardee:        203,795             193,420  95',
            '   Salt Springs:  140,000              68,603  49',
            '   Lower Bear:     49,000              34,076  70',
            ' New Hogan:       317,100   311,600   156,564  49       -155,036(  0)  0.67      ---',
            ' Farmington:       52,000         0       141   0            141(  0)  ----      ---',
            ' New Melones:   2,420,000 2,253,696 1,624,299  67       -629,397(  0)  ----       N/A',
            '   Beardsly:       97,800              63,567  65',
            '   Donnells:       64,300              20,117  31',
            ' Tulloch:          67,000    63,304    61,025  91         -2,279(  0)  ----      ---',
            ' Don Pedro:     2,030,000      -NR- 1,564,529  77           -NR-(  0)  ----       N/A',
            '   Hetch Hetchy:  360,400             280,538  78',
            '   Cherry Vly:    268,200             244,541  91',
            '   Eleanor:        26,000              18,428  71',
            ' New Exchequer: 1,024,600      -NR-   632,499  62           -NR-(  0)  ----       N/A',
            ' Burns:             6,800         0         3   0              3(  0)  ----      ---',
            ' Bear:              7,700         0         2   0              2(  0)  ----      ---',
            ' Owens:             3,600         0         0   0              0(  0)  ----      ---',
            ' Mariposa:         15,000         0         2   0              2(  0)  ----      ---',
            ' Los Banos:        34,600    20,562     9,836  28        -10,726(  0)  ----      ---',
            ' Buchanan:        150,000   134,159    14,697  10       -119,462(  0)  0.31      ---',
            ' Hidden:           90,000    67,142     7,300   8        -59,842(  0)  0.46      ---',
            ' Friant:          520,500   508,776   247,977  48       -260,799(  0)  ----       N/A',
            '   Mammoth:       122,700              58,784  48',
            '   Total U/S:     628,500             346,638  55',
            ' Big Dry Creek:    30,200       200       188   1            -12(  0)  ----      ---',
            ' ___________________________________________________________________________________',
            ' BASIN TOTALS   7,186,300 3,359,440 4,627,612  64      1,268,172( 33)',
            ' TOTAL FLOOD SPACE ENCROACHED                                148',
            ' w/US Storages  9,018,450           5,897,540  65',
            '',
            '                           ---- Tulare Lake Basin ----',
        ])
        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            mock_response.return_value.text = _with_fcr_header(dt.datetime(2025, 10, 2, tzinfo=ZoneInfo('US/Pacific')), report)
            result = partial(wcds.extract_basin_fcr_table, basin='San Joaquin Valley')(dt.datetime(2025, 10, 2, tzinfo=ZoneInfo('US/Pacific')))
            result_nr = partial(wcds.extract_basin_fcr_table, basin='San Joaquin Valley')(dt.datetime(2025, 10, 2, tzinfo=ZoneInfo('US/Pacific')),
                                                                 nr_value='NR')

        # the "EBMUD Reservoirs" group label has no values and is dropped
        self.assertEqual(len(result), 26)
        self.assertEqual(result.index[:3].tolist(), ['Camanche', 'Pardee', 'Salt Springs'])
        self.assertEqual(result.index[-1], 'Big Dry Creek')

        self.assertEqual(result.loc['Camanche'].tolist(), [417120.0, None, 308550.0, 74.0, None, 0.0, None, None])
        self.assertEqual(result_nr.loc['Camanche'].tolist(), [417120.0, 'NR', 308550.0, 74.0, 'NR', 0.0, None, None])
        self.assertEqual(result.loc['Pardee'].tolist(), [203795.0, None, 193420.0, 95.0] + [None] * 4)
        self.assertEqual(result.loc['New Hogan'].tolist(),
                         [317100.0, 311600.0, 156564.0, 49.0, -155036.0, 0.0, 0.67, None])
        self.assertEqual(result.loc['Farmington'].tolist(), [52000.0, 0.0, 141.0, 0.0, 141.0, 0.0, None, None])
        self.assertEqual(result.loc['Total U/S'].tolist(), [628500.0, None, 346638.0, 55.0] + [None] * 4)

        # non-Sacramento basins also return float columns where no values are missing or not reported
        self.assertEqual(result['Gross Pool (acft)'].dtype, float)
        self.assertEqual(result_nr['Gross Pool (acft)'].dtype, float)
        self.assertEqual(result_nr['Top of Conservation (acft)'].dtype, object)

    def _extract_basin_from_text(self, extract, section_lines, **kwargs):
        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            requested = dt.datetime(2025, 10, 2, tzinfo=ZoneInfo('US/Pacific'))
            mock_response.return_value.text = _with_fcr_header(requested, '\n'.join(section_lines))
            return extract(requested, **kwargs)

    def test_extract_basin_fcr_table_truckee_river_none_reported(self):
        """
        a column blank in every row (% of gross pool when no storages are reported) stays in place
        (report data ending 26 DEC 2025)
        """
        lines = [
            '                          ---- Truckee River Basin ----',
            '                -------------------- Storages -----------------------  Flood Control',
            '                   Gross    Top of     Actual  % of       Above Top     Parameters',
            '                   Pool  Conservation   Res    Gross     of Conserv    Rain     Snow',
            '                   (acft) (acft)***    (acft)  Pool         (acft)     (in)    (acft)',
            ' -------------- --------- --------- --------- -------- -------------- ---------------',
            ' Martis Creek:     20,400      -NR-      -NR-               -NR-(  0)  ----      ---',
            ' Prosser:          29,800      -NR-      -NR-               -NR-(  0)  ----       N/A',
            ' Boca:             41,100      -NR-      -NR-               -NR-(  0)  ----       N/A',
            ' Stampede:        226,500      -NR-      -NR-               -NR-(  0)  ----       N/A',
            ' ____________________________________________________________________________________',
            ' BASIN TOTALS     317,800         0         0   0              0(  0)',
            # the Truckee header omits "( **)"; the report labels percent encroached in its footnotes
            '',
            ' **  Percent Encroached',
        ]
        result = self._extract_basin_from_text(partial(wcds.extract_basin_fcr_table, basin='Truckee River Basin'), lines, nr_value='NR')
        self.assertEqual(result.index.tolist(), ['Martis Creek', 'Prosser', 'Boca', 'Stampede'])
        self.assertEqual(result.loc['Stampede'].tolist(), [226500.0, 'NR', 'NR', None, 'NR', 0.0, None, None])
        result = self._extract_basin_from_text(partial(wcds.extract_basin_fcr_table, basin='Truckee River Basin'), lines)
        self.assertEqual(result.loc['Martis Creek'].tolist(), [20400.0, None, None, None, None, 0.0, None, None])

    def test_extract_basin_fcr_table_so_san_francisco_bay(self):
        """
        a one-row table is parsed by column position; the parenthesized value after Del Valle's % of gross
        pool is not a table column (report data ending 19 NOV 2026)
        """
        lines = [
            '                       ---- So. San Francisco Bay Basin ----',
            '                -------------------- Storages -----------------------  Flood Control',
            '                   Gross    Top of     Actual  % of       Above Top     Parameters',
            '                   Pool  Conservation   Res    Gross     of Conserv    Rain     Snow',
            '                   (acft) (acft)***    (acft)  Pool       (acft)( **)  (in)    (acft)',
            ' -------------- --------- --------- --------- -------- -------------- ---------------',
            ' Del Valle:        77,000    39,000    27,797  36(  0)   -11,203(  0)  ----      ---',
            ' ____________________________________________________________________________________',
        ]
        result = self._extract_basin_from_text(partial(wcds.extract_basin_fcr_table, basin='So. San Francisco Bay Basin'), lines)
        self.assertEqual(result.index.tolist(), ['Del Valle'])
        self.assertEqual(result.loc['Del Valle'].tolist(), [77000.0, 39000.0, 27797.0, 36.0, -11203.0, 0.0, None, None])

    def test_extract_basin_unknown_basin(self):
        """
        a basin that is not an FCR report section name raises ValueError without querying WCDS
        """
        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            for extract in (wcds.extract_basin_fcr_table, wcds.extract_basin_totals):
                with self.subTest(extract=extract.__name__):
                    with self.assertRaisesRegex(ValueError, "basin must be one of .*; got 'Tulare'"):
                        extract(dt.datetime(2025, 1, 1, tzinfo=ZoneInfo('US/Pacific')), basin='Tulare')
            mock_response.assert_not_called()

    def test_extract_fcr_naive_datetime(self):
        """
        a naive datetime raises the timezone-aware assertion, not a naive/aware comparison TypeError
        """
        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            for extract in (partial(wcds.extract_basin_fcr_table, basin='Sacramento Valley'),
                            partial(wcds.extract_basin_totals, basin='Sacramento Valley'),
                            wcds._get_fcr_folsom_forecast):
                with self.subTest(extract=getattr(extract, 'func', extract).__name__):
                    with self.assertRaisesRegex(AssertionError, 'must be timezone aware'):
                        extract(dt.datetime(2025, 1, 1))
            mock_response.assert_not_called()

    def test_extract_basin_fcr_table_section_not_found(self):
        """
        a basin section missing from the report returns None
        """
        with mock.patch('builtins.print') as mock_print:
            result = self._extract_basin_from_text(partial(wcds.extract_basin_fcr_table, basin='Tulare Lake Basin'),
                                                   ['                           ---- San Joaquin Valley ----'])
        self.assertIsNone(result)
        self.assertIn('Tulare Lake Basin section not found', mock_print.call_args.args[0])

    def test_parse_fcr_value(self):
        """
        numbers keep their sign, -NR- is `nr_value`, and not-applicable values and unfilled placeholders are None
        """
        self.assertEqual(wcds._parse_fcr_value('-1,212,640'), -1212640.0)
        self.assertEqual(wcds._parse_fcr_value('(  0)'), 0.0)
        self.assertEqual(wcds._parse_fcr_value('(-142)'), -142.0)
        self.assertIsNone(wcds._parse_fcr_value('-NR-'))
        self.assertEqual(wcds._parse_fcr_value('-NR-', nr_value='NR'), 'NR')
        # whitespace padding inside or outside the parentheses
        for value, expected in ((' ( -142 ) ', -142.0), ('(\t5)', 5.0), ('( -NR- )', 'NR'), ('(-NR-\t)', 'NR'),
                                ('\t-NR-\n', 'NR')):
            with self.subTest(value=value):
                self.assertEqual(wcds._parse_fcr_value(value, nr_value='NR'), expected)
        # not applicable, blank, and unfilled placeholders (codes ending in SP, template variables)
        for value in ('----', '---', 'N/A', '', None, 'NHSP', 'PFSP', '%NEXTCS', '****'):
            with self.subTest(value=value):
                self.assertIsNone(wcds._parse_fcr_value(value, nr_value='NR'))

    def test_extract_fcr_data_cutoff(self):
        """
        each basin section's tables are unavailable before the first report with valid values for most of the
        section: earlier requests return None without querying WCDS, and midnight US/Pacific on the start date
        queries the report
        """
        starts = {'Sacramento Valley': dt.date(2014, 2, 5),
                  'San Joaquin Valley': dt.date(2014, 5, 27),
                  'Tulare Lake Basin': dt.date(2013, 8, 19),
                  'Truckee River Basin': dt.date(2014, 5, 27),
                  'Russian River Basin': dt.date(2014, 1, 13),
                  'So. San Francisco Bay Basin': dt.date(2014, 5, 27)}
        tables = [(basin, basin, start, partial(wcds.extract_basin_fcr_table, basin=basin))
                  for basin, start in starts.items()]
        tables += [(f'{basin} basin totals', basin, start, partial(wcds.extract_basin_totals, basin=basin))
                   for basin, start in starts.items()]
        tables += [('Folsom forecast volumes', 'Sacramento Valley', dt.date(2019, 7, 1), wcds._get_fcr_folsom_forecast)]
        pacific = ZoneInfo('US/Pacific')
        for table, basin, start, extract in tables:
            midnight = dt.datetime(start.year, start.month, start.day, tzinfo=pacific)
            with (self.subTest(table=table, extract=getattr(extract, '__name__', 'partial')),
                  mock.patch.object(wcds.utils, 'get_session_response') as mock_response,
                  mock.patch('builtins.print') as mock_print):
                # one minute before midnight on the start date, in US/Pacific and in UTC
                for before in (midnight - dt.timedelta(minutes=1),
                               (midnight - dt.timedelta(minutes=1)).astimezone(ZoneInfo('UTC'))):
                    self.assertIsNone(extract(before))
                    self.assertEqual(mock_print.call_args.args[0],
                                     f'WARNING: {table} table unavailable before {start:%Y-%m-%d}')
                mock_response.assert_not_called()

                mock_response.return_value.text = _with_fcr_header(midnight, '')
                extract(midnight)
                mock_response.assert_called_once()

    def test_extract_basin_fcr_table_sacramento_valley_unlabeled_percent_encroached(self):
        """
        parenthesized values are not read as percent encroached when the report does not label them, with
        the "( **)" header or the "**  Percent Encroached" footnote (report data ending 05 FEB 2014)
        """
        report = '\n'.join([
            '                           ---- Sacramento Valley ----',
            '                -------------------- Storages -----------------------  Flood Control',
            '                   Gross    Top of     Actual  % of       Above Top     Parameters',
            '                   Pool  Conservation   Res    Gross     of Conserv    Rain     Snow',
            '                   (acft) (acft)***    (acft)  Pool         (acft)     (in)    (acft)',
            ' -------------- --------- --------- --------- -------- -------------- ---------------',
            ' Shasta:        4,552,100 3,909,571 1,653,860  36     -2,255,711 (86)  58463 CFS',
            ' Black Butte:     136,200    69,800    13,560  10        -56,240 (51)  0.28      ---',
            '   East Park:      50,900               7,214  14',
            '   Stony Gorge:    50,000              14,251  29',
            ' Oroville:      3,538,000 3,163,000      ****               -NR- (89)  0.08      ---',
            ' New Bullards:    966,000   796,000   402,504  42       -393,496 (82)  ----      ---',
            ' Indian Valley:   300,600   260,600    13,000   4       -247,600       ----      ---',
            '*Folsom:          977,000   577,000   162,652  17       -414,348 (59)  0.91      ---',
            '                           ---- San Joaquin Valley ----',
        ])
        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            mock_response.return_value.text = _with_fcr_header(dt.datetime(2014, 2, 5, tzinfo=ZoneInfo('US/Pacific')), report)
            result = wcds.extract_basin_fcr_table(dt.datetime(2014, 2, 5, tzinfo=ZoneInfo('US/Pacific')), 'Sacramento Valley')
        # the row reads "-2,255,711 (86)", which cannot be a percent encroached below top of conservation
        self.assertEqual(result.loc['Shasta'].tolist(),
                         [4552100.0, 3909571.0, 1653860.0, 36.0, -2255711.0, None, 58463.0, None])
        self.assertTrue(all(value is None for value in result[r'% Encroached']))

    def test_extract_basin_fcr_table_sacramento_valley_2014_format(self):
        """
        the earlier report format, with -NR- values without percent encroached and a full Folsom row
        (report data ending 16 NOV 2014)
        """
        report = '\n'.join([
            '                           ---- Sacramento Valley ----',
            '                -------------------- Storages -----------------------  Flood Control',
            '                   Gross    Top of     Actual  % of       Above Top     Parameters',
            '                   Pool  Conservation   Res    Gross     of Conserv    Rain     Snow',
            '                   (acft) (acft)***    (acft)  Pool         (acft)     (in)    (acft)',
            ' -------------- --------- --------- --------- -------- -------------- ---------------',
            ' Shasta:        4,552,100      -NR- 1,059,352  23           -NR-       -NR-',
            ' Black Butte:     136,200      -NR-      -NR-               -NR-       -NR-      ---',
            '   East Park:      50,900               7,782  15',
            '   Stony Gorge:    50,000               8,300  17',
            ' Oroville:      3,538,000      -NR-      -NR-               -NR-       -NR-      ---',
            ' New Bullards:    966,000      -NR-      -NR-               -NR-       ----      ---',
            ' Indian Valley:   300,600      -NR-      -NR-               -NR-       ----      ---',
            '*Folsom:          977,000      -NR-   284,757  29           -NR-       -NR-      ---',
            '                           ---- San Joaquin Valley ----',
        ])
        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            mock_response.return_value.text = _with_fcr_header(dt.datetime(2014, 11, 16, tzinfo=ZoneInfo('US/Pacific')), report)
            result = wcds.extract_basin_fcr_table(dt.datetime(2014, 11, 16, tzinfo=ZoneInfo('US/Pacific')), 'Sacramento Valley',
                                                      nr_value='NR')
        self.assertEqual(result.index.tolist(), ['Shasta', 'Black Butte', 'East Park', 'Stony Gorge', 'Oroville',
                                                 'New Bullards', 'Indian Valley', 'Folsom'])
        self.assertEqual(result.loc['Shasta'].tolist(), [4552100.0, 'NR', 1059352.0, 23.0, 'NR', None, 'NR', None])
        self.assertEqual(result.loc['East Park'].tolist(), [50900.0, None, 7782.0, 15.0, None, None, None, None])
        self.assertEqual(result.loc['Folsom'].tolist(), [977000.0, 'NR', 284757.0, 29.0, 'NR', None, 'NR', None])

    def test_get_fcr_data_table_failure(self):
        """
        tables are returned by basin section, and a table that fails to parse is returned as None without discarding
        the other tables; the report is retrieved once and passed to each table
        """
        requested = dt.datetime.now(tz=ZoneInfo('US/Pacific')) - dt.timedelta(days=2)

        def basin_fcr(*args, basin, **kwargs):
            if basin == 'Truckee River Basin':
                raise ValueError('bad row')
            return f'{basin} fcr'

        with (mock.patch.object(wcds.utils, 'get_session_response') as mock_response,
              mock.patch.object(wcds, '_get_fcr_folsom_forecast', side_effect=IndexError('list index out of range')),
              mock.patch.object(wcds, 'extract_basin_totals', side_effect=lambda *args, basin, **kwargs:
                                f'{basin} totals') as mock_totals,
              mock.patch.object(wcds, 'extract_basin_fcr_table', side_effect=basin_fcr) as mock_basin_fcr,
              mock.patch.object(wcds, 'extract_basin_footnotes', side_effect=lambda *args, basin, **kwargs:
                                f'{basin} footnotes') as mock_footnotes,
              mock.patch('builtins.print') as mock_print):
            mock_response.return_value.text = _with_fcr_header(requested, 'report text')
            result = wcds.get_fcr_data(requested, nr_value='NR')

        expected = {'date': f'{requested:%Y-%m-%d}', 'generated': None}
        expected.update({basin: {'fcr': f'{basin} fcr', 'totals': f'{basin} totals', 'footnotes': f'{basin} footnotes'}
                         for basin in wcds.FCR_BASINS})
        expected['Sacramento Valley']['folsom'] = None
        expected['Truckee River Basin']['fcr'] = None
        self.assertEqual(result, expected)
        warnings = [call.args[0] for call in mock_print.call_args_list]
        self.assertTrue(any('FCR `Sacramento Valley folsom` table could not be parsed' in w for w in warnings))
        self.assertTrue(any('FCR `Truckee River Basin fcr` table could not be parsed' in w for w in warnings))

        # one request; every table gets the same report text and nr_value
        mock_response.assert_called_once()
        for basin in wcds.FCR_BASINS:
            mock_basin_fcr.assert_any_call(requested, nr_value='NR', report=mock_response.return_value.text,
                                           basin=basin)
            mock_totals.assert_any_call(requested, nr_value='NR', report=mock_response.return_value.text, basin=basin)
            # the footnotes are text, so they take no nr_value
            mock_footnotes.assert_any_call(requested, report=mock_response.return_value.text, basin=basin)

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
