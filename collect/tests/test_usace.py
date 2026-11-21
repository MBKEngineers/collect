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
            '',
            ' **  Percent Encroached',
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

    def test_extract_san_joaquin_valley_fcr_data(self):
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
            mock_response.return_value.text = report
            result = wcds.extract_san_joaquin_valley_fcr_data(dt.datetime(2025, 10, 2, tzinfo=ZoneInfo('US/Pacific')))
            result_nr = wcds.extract_san_joaquin_valley_fcr_data(dt.datetime(2025, 10, 2, tzinfo=ZoneInfo('US/Pacific')),
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

    def _extract_basin_from_text(self, extract, section_lines, **kwargs):
        with mock.patch.object(wcds.utils, 'get_session_response') as mock_response:
            mock_response.return_value.text = '\n'.join(section_lines)
            return extract(dt.datetime(2025, 10, 2, tzinfo=ZoneInfo('US/Pacific')), **kwargs)

    def test_extract_truckee_river_basin_fcr_data_none_reported(self):
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
        result = self._extract_basin_from_text(wcds.extract_truckee_river_basin_fcr_data, lines, nr_value='NR')
        self.assertEqual(result.index.tolist(), ['Martis Creek', 'Prosser', 'Boca', 'Stampede'])
        self.assertEqual(result.loc['Stampede'].tolist(), [226500.0, 'NR', 'NR', None, 'NR', 0.0, None, None])
        result = self._extract_basin_from_text(wcds.extract_truckee_river_basin_fcr_data, lines)
        self.assertEqual(result.loc['Martis Creek'].tolist(), [20400.0, None, None, None, None, 0.0, None, None])

    def test_extract_so_san_francisco_bay_basin_fcr_data(self):
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
        result = self._extract_basin_from_text(wcds.extract_so_san_francisco_bay_basin_fcr_data, lines)
        self.assertEqual(result.index.tolist(), ['Del Valle'])
        self.assertEqual(result.loc['Del Valle'].tolist(), [77000.0, 39000.0, 27797.0, 36.0, -11203.0, 0.0, None, None])

    def test_extract_basin_fcr_data_section_not_found(self):
        """
        a basin section missing from the report returns None
        """
        with mock.patch('builtins.print') as mock_print:
            result = self._extract_basin_from_text(wcds.extract_tulare_lake_basin_fcr_data,
                                                   ['                           ---- San Joaquin Valley ----'])
        self.assertIsNone(result)
        self.assertIn('Tulare Lake Basin section not found', mock_print.call_args.args[0])

    def test_extract_fcr_data_cutoff(self):
        """
        FCR tables are unavailable before 2014-02-05 00:00 US/Pacific: earlier requests return None without
        querying WCDS
        """
        functions = {'Sac Valley': wcds.extract_sac_valley_fcr_data,
                     'Sacramento Valley basin totals': wcds.extract_basin_totals,
                     'San Joaquin Valley': wcds.extract_san_joaquin_valley_fcr_data,
                     'Tulare Lake Basin': wcds.extract_tulare_lake_basin_fcr_data,
                     'Truckee River Basin': wcds.extract_truckee_river_basin_fcr_data,
                     'Russian River Basin': wcds.extract_russian_river_basin_fcr_data,
                     'So. San Francisco Bay Basin': wcds.extract_so_san_francisco_bay_basin_fcr_data}
        for table, extract in functions.items():
            with self.subTest(table=table), \
                 mock.patch.object(wcds.utils, 'get_session_response') as mock_response, \
                 mock.patch('builtins.print') as mock_print:
                for before in (dt.datetime(2014, 2, 4, 23, 59, tzinfo=ZoneInfo('US/Pacific')),
                               # 2014-02-05 07:59 UTC is still 2014-02-04 in US/Pacific
                               dt.datetime(2014, 2, 5, 7, 59, tzinfo=ZoneInfo('UTC'))):
                    self.assertIsNone(extract(before))
                    self.assertEqual(mock_print.call_args.args[0],
                                     f'WARNING: {table} table unavailable before 2014-02-05')
                mock_response.assert_not_called()

                # midnight on the cutoff date in US/Pacific queries the report
                mock_response.return_value.text = ''
                try:
                    extract(dt.datetime(2014, 2, 5, tzinfo=ZoneInfo('US/Pacific')))
                except IndexError:
                    pass  # extract_basin_totals indexes the (empty) report text
                mock_response.assert_called_once()

    def test_extract_sac_valley_fcr_data_unlabeled_percent_encroached(self):
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
            mock_response.return_value.text = report
            result = wcds.extract_sac_valley_fcr_data(dt.datetime(2014, 2, 5, tzinfo=ZoneInfo('US/Pacific')))
        # the row reads "-2,255,711 (86)", which cannot be a percent encroached below top of conservation
        self.assertEqual(result.loc['Shasta'].tolist(),
                         [4552100.0, 3909571.0, 1653860.0, 36.0, -2255711.0, None, 58463.0, None])
        self.assertTrue(all(value is None for value in result[r'% Encroached']))

    def test_extract_sac_valley_fcr_data_2014_format(self):
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
            mock_response.return_value.text = report
            result = wcds.extract_sac_valley_fcr_data(dt.datetime(2014, 11, 16, tzinfo=ZoneInfo('US/Pacific')),
                                                      nr_value='NR')
        self.assertEqual(result.index.tolist(), ['Shasta', 'Black Butte', 'East Park', 'Stony Gorge', 'Oroville',
                                                 'New Bullards', 'Indian Valley', 'Folsom'])
        self.assertEqual(result.loc['Shasta'].tolist(), [4552100.0, 'NR', 1059352.0, 23.0, 'NR', None, 'NR', None])
        self.assertEqual(result.loc['East Park'].tolist(), [50900.0, None, 7782.0, 15.0, None, None, None, None])
        self.assertEqual(result.loc['Folsom'].tolist(), [977000.0, 'NR', 284757.0, 29.0, 'NR', None, 'NR', None])

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
