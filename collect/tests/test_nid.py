"""
collect.tests.test_nid
============================================================
initial test suite for collect.nid data access and utility functions; note: these tests require internet connection
"""
# -*- coding: utf-8 -*-
import datetime as dt
import io
import textwrap
import unittest
import unittest.mock
import pandas as pd
from collect import nid


class TestNID(unittest.TestCase):

    @property
    def sample_daily_data(self):
        if not hasattr(self, '_sample_daily_data'):
            self._sample_daily_data = io.StringIO(textwrap.dedent("""\
                Nevada Irrigation District                                                                             USDAY V123  Output 11/22/2023

                                                                           Summary Report                                                           

                Site:           DC900 Scott's Flat Reservoir
                USGS #:         
                Beginning Date: 01/01/2023
                Ending Date:    12/31/2023

                                               Daily 2400 Storage Volume in Acre-Feet Water Year Jan 2023 to Dec 2023                               

                12/31/2022    44500

                Day             JAN       FEB       MAR       APR       MAY       JUN       JUL       AUG       SEP       OCT       NOV       DEC
                ------------------------------------------------------------------------------------------------------------------------------------
                 1            45300     48500     48500     48500     48500     47800     47700     45100     42400     40100                    
                 2            45800     48500     48500     48500     48500     47800     47600     45000     42300     40100                    
                 3            46200     48500     48500     48500     48500     47900     47500     44900     42300     40000                    
                 4            46400     48500     48500     48500     48500     48000     47500     44800     42200     40000                    
                 5            46900     48500     48500     48500     48500     48000     47400     44700     42100     39900                    

                 6            47300     48500     48500     48500     48500     48000     47400     44600     42000     39900                    
                 7            47500     48500     48500     48500     48500     48100     47300     44500     42000     39800                    
                 8            47800     48500     48500     48500     48500     48000     47200     44400     41900     39800                    
                 9            48500     48500     48500     48500     48500     48000     47200     44400     41800     39800                    
                10            48500     48500     48500     48500     48500     48000     47100     44300     41700     39700                    

                11            48500     48500     48500     48500     48500     48000     47000     44200     41600     39700                    
                12            48500     48500     48500     48500     48500     48000     47000     44100     41500     39700                    
                13            48500     48500     48500     48500     48400     48100     46900     44000     41400     39600                    
                14            48500     48500     48500     48500     48400     48100     46800     43900     41400     39600                    
                15            48500     48500     48500     48500     48300     48100     46700     43800     41300     39700                    

                16            48500     48500     48500     48500     48300     48100     46600     43700     41200     39800                    
                17            48500     48500     48500     48500     48200     48100     46500     43600     41100     39800                    
                18            48500     48500     48500     48500     48100     48000     46400     43500     41000     39900                    
                19            48500     48500     48500     48500     48100     48000     46400     43400     40900     40000                    
                20            48500     48400     48500     48500     48000     48000     46300     43400     40900     40100                    

                21            48500     48400     48500     48500     47900     48000     46200     43300     40800     40100                    
                22            48500     48400     48500     48500     47800     48000     46100     43200     40700     40200                    
                23            48500     48300     48500     48500     47800     47900     46000     43200     40600                              
                24            48500     48400     48500     48500     47700     47900     45900     43100     40600                              
                25            48500     48300     48500     48500     47600     47900     45800     43000     40500                              

                26            48500     48400     48500     48500     47600     47900     45700     42900     40400                              
                27            48500     48500     48500     48500     47600     47800     45600     42800     40400                              
                28            48500     48500     48500     48500     47600     47800     45500     42700     40300                              
                29            48500    ------     48500     48500     47700     47700     45400     42600     40300                              
                30            48500    ------     48500     48500     47700     47700     45300     42500     40200                              
                31            48500    ------     48500    ------     47700    ------     45200     42400    ------              ------          

                Max           48500     48500     48500     48500     48500     48100     47700     45100     42400     40200                    
                Min           45300     48300     48500     48500     47600     47700     45200     42400     40200     39600                    
                Change         4000         0         0         0      -800         0     -2500     -2800     -2200                              

                Cal Year 2023     Mean     46300      Max     48500      Min     39600 Inst Max     48500

                                                        ------------------ Notes -------------------
                                                        All recorded data is continuous and reliable
            """))
        return self._sample_daily_data

    def test_get_sites(self):
        result = nid.get_sites()
        expected_dict = {'BR100': 'Auburn Ravine I at Head',
                         'BR220': 'Hemphill Canal at Head',
                         'BR301': 'Combie Phase I at Head',
                         'BR334': 'Camp Far West at Head',
                         'BR368': 'Gold Hill I at Head',
                         'BR900': 'Combie Reservoir-Spill-1600.',
                         'BSCA': 'Bowman-Spaulding Canal Intake Near Graniteville, Ca',
                         'BWMN': 'Bowman Lake Near Graniteville, Ca',
                         'CPFL': 'Chicago Park Flume Near Dutch Flat, Ca',
                         'DC102': 'Cascade at Head',
                         'DC131': 'Newtown Canal at Head',
                         'DC140': 'Tunnel Canal at Head',
                         'DC145': 'D. S. Canal at Head',
                         'DC169': 'Tarr Canal at Head',
                         'DC900': "Scott's Flat Reservoir",
                         'DFFL': 'Dutch Flat #2 Flume Near Blue Canyon, Ca',
                         'FAUC': 'Faucherie Lake Near Cisco, Ca',
                         'FRLK': 'French Lake Near Cisco Grove, Ca',
                         'JKSN': 'Jackson Lake near Sierra City',
                         'JMDW': 'Jackson Meadows Reservoir Near Sierra City, Ca',
                         'MBTO': 'Milton-Bowman Tunnel Outlet (South Portal)',
                         'ROLK': 'Rollins Reservoir Near Colfax, Ca',
                         'SWML': 'Sawmill Lake Near Graniteville, Ca',
                         'WLSN': 'Wilson Creek near Sierra City'}
        self.assertEqual(result, expected_dict)

    def test_get_issue_date(self):
        result = nid.get_issue_date()
        self.assertTrue(isinstance(result, dt.datetime))
        self.assertLess(result, dt.datetime.now())

    def test_get_site_files(self):
        site = 'DC140'
        result = nid.get_site_files('DC140')
        self.assertEqual(sorted(result), [f'{site}.adesc.pdf',
                                          f'{site}.csv_flow.csv',
                                          f'{site}.plot_flow.png',
                                          f'{site}.usday_daily_flow.txt'])

    def test_get_site_metric(self):
        self.assertEqual(nid.get_site_metric('BR334', interval='daily'), 'flow')

    def test_get_station_url(self):
        self.assertEqual(nid.get_station_url('ROLK', metric='index', interval=None),
                         'https://river-lake.nidwater.com/hyquick/ROLK/index.htm')

        self.assertEqual(nid.get_station_url('ROLK', metric='flow', interval='daily'),
                         'https://river-lake.nidwater.com/hyquick/ROLK/ROLK.usday_daily_flow.txt')

        self.assertEqual(nid.get_station_url('ROLK', metric='flow', interval='hourly'),
                         'https://river-lake.nidwater.com/hyquick/ROLK/ROLK.csv_flow.csv')

    def test_get_daily_data(self):
        result = nid.get_daily_data('DC900', json_compatible=False)
        year = result['info']['year']
        self.assertEqual(result['data'].head(4).index.strftime('%Y-%m-%d').tolist(),
                         [f'{year}-01-01', f'{year}-01-02', f'{year}-01-03', f'{year}-01-04'])

    def test_get_daily_meta(self):
        url = 'https://river-lake.nidwater.com/hyquick/DC140/DC140.usday_daily_flow.txt'
        result = nid.get_daily_meta(url=url, content=None)
        self.assertEqual(result['Site'], 'DC140 Tunnel Canal at Head')
        self.assertEqual(result['USGS #'], 'NO')
        self.assertEqual(result['version'], 'USDAY V123')

    def test_get_hourly_data(self):
        result = nid.get_hourly_data('WLSN', json_compatible=False)
        sample = result['data'].head()
        self.assertEqual(sample.index.strftime('%Y-%m-%d %H:%M:%S').tolist(),
                        ['2022-01-01 01:00:00',
                         '2022-01-01 02:00:00',
                         '2022-01-01 03:00:00',
                         '2022-01-01 04:00:00',
                         '2022-01-01 05:00:00'])
        self.assertEqual(sample['Amount Diverted (AF)'].tolist(), [0.15, 0.15, 0.15, 0.15, 0.15])

    def test_parse_qualifiers(self):
        series = pd.Series(data=['Qualities:',
                            '2 - Good quality edited data',
                            '22 - Raw Satellite Data',
                            '28 - Radio data',
                            '255 - No data exists'],
                           name='Site Information')
        self.assertEqual(nid.parse_qualifiers(series),
                         {'2': 'Good quality edited data',
                          '22': 'Raw Satellite Data',
                          '28': 'Radio data',
                          '255': 'No data exists'})

    def test_serialize(self):
        df = pd.DataFrame(index=pd.date_range('2020-12-01', '2020-12-03', freq='D'),
                          data={'VALUE': [42] * 3})
        self.assertEqual(nid.serialize(df.copy(), day_format='%Y-%m-%d'),
                        {'VALUE': {'2020-12-01 00:00': 42, '2020-12-02 00:00': 42, '2020-12-03 00:00': 42}})
        self.assertEqual(nid.serialize(df.copy(), day_format='%Y-%-m-%-d'),
                        {'VALUE': {'2020-12-1 00:00': 42, '2020-12-2 00:00': 42, '2020-12-3 00:00': 42}})


if __name__ == '__main__':
    unittest.main()
