"""
collect.tests.test_cvo
============================================================
initial test suite for collect.cvo data access and utility functions; note: these tests require internet connection
"""
# -*- coding: utf-8 -*-
import datetime as dt
import io
import os
import textwrap
import unittest
import unittest.mock

import pandas as pd

from collect import cvo
from collect import utils


class TestCVO(unittest.TestCase):

    def test_get_area(self):
        """
        demonstrate that get_area produces expected results for all reports for one possible date
        """
        self.assertEqual(cvo.get_area(dt.date(2013, 12, 1), 'doutdly'), [151.19, 20.76, 390, 900.67])
        self.assertEqual(cvo.get_area(dt.date(2013, 12, 1), 'fedslu'), [140, 30, 500, 700])
        self.assertEqual(cvo.get_area(dt.date(2013, 12, 1), 'kesdop'), [145, 30, 465, 881])
        self.assertEqual(cvo.get_area(dt.date(2013, 12, 1), 'shadop'),  [140, 30, 700, 540])
        self.assertEqual(cvo.get_area(dt.date(2013, 12, 1), 'shafln'), [140, 30, 445, 540])
        self.assertEqual(cvo.get_area(dt.date(2013, 12, 1), 'slunit'), [120, 20, 480, 820])

    def test_get_data(self):
        """
        initial tests to demonstrate retrieving data spanning multiple PDF reports to build a timeseries record
        """
        result = cvo.get_data(dt.date(2023, 6, 1), dt.date(2023, 8, 31), 'shadop')
        self.assertEqual(result['data'].sum()['ELEV']['ELEV']['ELEV'], 96536.34)
        self.assertEqual(result['data'].shape, (92, 11))

    def test_get_date_published(self):
        """
        test that date published can be extracted from a past report in the archive
        """
        url = cvo.get_url(dt.date(2022, 2, 15), 'shadop')
        result = cvo.get_date_published(url, dt.date(2022, 2, 15), 'shadop')
        self.assertEqual(result.strftime('%Y-%m-%d'), '2023-04-19')
        self.assertTrue(isinstance(result, dt.date))

    def test_get_report_columns(self):
        """
        demonstration of expected behavior for get_report_columns with shafln report type
        """
        expected_result = (
            ('Day', ''),
            ('Storage - A.F.', 'Lake Britton'),
            ('Storage - A.F.', 'McCloud Div'),
            ('Storage - A.F.', 'Iron Canyon'),
            ('Storage - A.F.', 'Pit 6'),
            ('Storage - A.F.', 'Pit 7'),
            ('Reservoir Total', 'Reservoir Total'),
            ('Change', 'A.F.'),
            ('Change', 'C.F.S.'),
            ('Shasta Inflow C.F.S.', 'Shasta Inflow C.F.S.'),
            ('Natural River C.F.S.', 'Natural River C.F.S.'),
            ('Accum * Full Natural 1000 A.F.', 'Accum * Full Natural 1000 A.F.')
        )
        self.assertEqual(cvo.get_report_columns('shafln', dt.date.today(), expected_length=None, default=False),
                         expected_result)

    def test_get_report(self):
        """
        test demonstrating expected behavior for delta daily outflow report retrieval for a particular date (May 2022)
        """
        result = cvo.get_report(dt.date(2022, 5, 1), 'doutdly')
        sample = result['data'].head()['Outflow Index']['Monthly Avg']
        self.assertEqual(sample.index.strftime('%Y-%m-%d').tolist(),
                         ['2022-05-14', '2022-05-15', '2022-05-16', '2022-05-17', '2022-05-18'])
        self.assertEqual(sample.values.tolist(),
                         [4473.0, 4540.0, 4557.0, 4606.0, 4640.0])

    def test_get_title(self):
        """
        test that the correct title is provided for each supported report type
        """
        for report_type, expected_title in [
            ('doutdly', 'U.S. Bureau of Reclamation - Central Valley Operations Office Delta Outflow Computation'),
            ('fedslu', 'San Luis Reservoir Federal Daily Operations'),
            ('kesdop', 'Kesdop Reservoir Daily Operations'),
            ('shadop', 'Shadop Reservoir Daily Operations'),
            ('shafln', 'Shasta Reservoir Daily Operations'),
            ('slunit', 'Federal-State Operations, San Luis Unit')
        ]:
            self.assertEqual(cvo.get_title(report_type), expected_title)

    @unittest.mock.patch('collect.cvo.dt.date')
    def test_get_url_today(self, mock_date):
        """
        test that the correct url is returned if the provided date_structure is mocked to represent "today"
        """
        mock_date.today.return_value = dt.date(2023, 11, 1)
        expected_url = 'https://www.usbr.gov/mp/cvo/vungvari/fedslu.pdf'
        self.assertEqual(cvo.get_url(dt.date(2023, 11, 1), 'fedslu'), expected_url)

    def test_get_url(self):
        """
        test that the correct url is returned for a report with either date or datetime input that is not "today"
        """
        expected_url = 'https://www.usbr.gov/mp/cvo/vungvari/fedslu0120.pdf'
        self.assertEqual(cvo.get_url(dt.date(2020, 1, 1), 'fedslu'), expected_url)
        self.assertEqual(cvo.get_url(dt.datetime(2020, 1, 1), 'fedslu'), expected_url)

    def test_months_between(self):
        """
        test that the generator yields the appropriate sequence of months
        """
        self.assertEqual(list(cvo.months_between(dt.datetime(2023, 1, 1), dt.datetime(2023, 4, 1))),
                         [dt.date(2023, 1, 1), dt.date(2023, 2, 1), dt.date(2023, 3, 1), dt.date(2023, 4, 1)])
        self.assertEqual(list(cvo.months_between(dt.date(2023, 1, 1), dt.date(2023, 4, 1))),
                         [dt.date(2023, 1, 1), dt.date(2023, 2, 1), dt.date(2023, 3, 1), dt.date(2023, 4, 1)])

    def test_doutdly_data_cleaner(self):
        content = io.StringIO(textwrap.dedent("""\
            02/01/20,"21,152",210.0,93.0,579.0,"2,058","2,090","2,055","24,092",900.0,"3,696","2,617",186.0,4.0,45.0,"6,540","6,342","16,652","17,092","16,652",26%,24%,30%
            02/02/20,"19,222",217.0,84.0,536.0,"2,055","2,074","2,040","22,114",900.0,"3,691","2,612",168.0,5.0,58.0,"6,524","6,325","14,690","17,310","15,671",28%,26%,29%
            02/03/20,"17,787",224.0,76.0,523.0,"2,025","2,062","2,038","20,635",900.0,"3,694","2,626",136.0,4.0,69.0,"6,520","6,312","13,215","17,168","14,852",31%,28%,29%
            02/04/20,"17,439",231.0,70.0,515.0,"2,034","2,083","2,090","20,289",900.0,"3,700","2,621",153.0,4.0,62.0,"6,531","6,314","12,858","16,839","14,354",31%,30%,29%
            02/05/20,"16,595",239.0,65.0,501.0,"2,245","2,191","2,240","19,645",900.0,"2,696","3,478",154.0,3.0,58.0,"6,384","6,271","12,361","15,842","13,955",31%,31%,29%
            02/06/20,"16,158",246.0,63.0,497.0,"2,840","2,380","2,434","19,804",900.0,"2,896","3,480",126.0,4.0,59.0,"6,558","6,290","12,346","14,560","13,687",32%,32%,29%
            02/07/20,"15,874",253.0,60.0,486.0,"3,403","2,550","2,550","20,076",900.0,"3,393","3,555",128.0,12.0,66.0,"7,130","6,499","12,046","13,452","13,452",35%,33%,30%
            02/08/20,"15,134",260.0,57.0,477.0,"3,248","2,658","2,582","19,176",900.0,"2,989","3,597",104.0,10.0,79.0,"6,758","6,637","11,518","12,719","13,211",34%,34%,30%
            02/09/20,"14,607",260.0,54.0,469.0,"2,809","2,720","2,569","18,199",900.0,"2,796","2,636",108.0,12.0,57.0,"5,585","6,322","11,714","12,294","13,044",30%,33%,29%
            02/10/20,"14,662",260.0,51.0,463.0,"2,464","2,740","2,529","17,900",900.0,"2,798","2,629",104.0,12.0,128.0,"5,647","5,815","11,353","12,028","12,875",30%,31%,27%
            02/11/20,"13,321",260.0,47.0,455.0,"2,171","2,698","2,477","16,254",850.0,"1,193","2,645",110.0,12.0,69.0,"4,005","4,899","11,399","11,819","12,741",24%,28%,23%
            02/12/20,"13,006",260.0,44.0,450.0,"1,952","2,555","2,424","15,712",850.0,"1,088","1,845",113.0,20.0,73.0,"3,099","4,066","11,763","11,734","12,659",19%,24%,20%
            02/13/20,"12,921",260.0,43.0,443.0,"1,841","2,315","2,370","15,508",850.0,"1,992",879,120.0,13.0,72.0,"3,051","3,214","11,607","11,628","12,579",18%,20%,16%
            02/14/20,"12,529",260.0,41.0,438.0,"1,720","2,083","2,316","14,988",850.0,"1,486",876,105.0,21.0,69.0,"2,516","2,722","11,622","11,568","12,510",16%,18%,14%
            02/15/20,"12,749",260.0,41.0,435.0,"1,623","1,896","2,262","15,108",850.0,"1,486",876,111.0,16.0,67.0,"2,524","2,532","11,734","11,599","12,458",16%,17%,14%
            02/16/20,"13,044",260.0,41.0,433.0,"1,500","1,750","2,211","15,278",850.0,"1,690",876,109.0,16.0,71.0,"2,730","2,430","11,698","11,597","12,411",17%,16%,14%
            02/17/20,"12,770",260.0,41.0,431.0,"1,444","1,643","2,164","14,946",850.0,"1,491",877,111.0,13.0,68.0,"2,533","2,432","11,563","11,626","12,361",16%,16%,14%
            02/18/20,"12,694",260.0,40.0,429.0,"1,422","1,563","2,122","14,845",850.0,"1,399",875,110.0,6.0,74.0,"2,452","2,403","11,543","11,647","12,316",15%,16%,14%
            02/19/20,"11,719",260.0,39.0,425.0,"1,394","1,498","2,083","13,837",850.0,692,878,112.0,16.0,77.0,"1,743","2,071","11,244","11,573","12,259",11%,14%,12%
            02/20/20,"11,472",260.0,38.0,423.0,"1,384","1,452","2,049","13,577",850.0,0,877,112.0,19.0,79.0,"1,050","1,574","11,677","11,583","12,230",6%,11%,10%
            02/21/20,"11,312",260.0,37.0,421.0,"1,397","1,498","2,044","13,427",850.0,698,4,109.0,15.0,65.0,860,"1,049","11,717","11,596","12,206",5%,8%,7%
            02/22/20,"11,089",260.0,37.0,415.0,"1,943","1,601","2,052","13,744",900.0,493,870,105.0,21.0,73.0,"1,520",980,"11,324","11,538","12,166",10%,7%,6%
            02/23/20,"10,725",260.0,40.0,412.0,"2,222","1,760","2,074","13,659",900.0,495,873,116.0,18.0,72.0,"1,538","1,144","11,221","11,470","12,124",10%,8%,8%
            02/24/20,"11,437",260.0,39.0,412.0,"2,556","1,944","2,100","14,704",900.0,792,871,116.0,27.0,74.0,"1,826","1,464","11,978","11,529","12,118",11%,10%,10%
            02/25/20,"11,846",260.0,35.0,419.0,"2,709","2,137","2,126","15,269",900.0,"1,689",870,120.0,43.0,80.0,"2,716","1,863","11,653","11,545","12,100",16%,13%,13%
            02/26/20,"11,768",260.0,33.0,375.0,"2,751","2,330","2,149","15,187",900.0,"1,773",869,120.0,42.0,78.0,"2,798","2,288","11,489","11,580","12,076",17%,15%,15%
            02/27/20,"11,975",260.0,33.0,393.0,"2,732","2,527","2,173","15,393",900.0,"1,797",870,118.0,35.0,69.0,"2,820","2,623","11,673","11,579","12,061",17%,17%,18%
            02/28/20,"11,881",260.0,31.0,299.0,"2,777","2,646","2,194","15,248",900.0,"1,791",876,121.0,32.0,68.0,"2,824","2,659","11,524","11,552","12,042",17%,17%,18%
            02/29/20,"11,767",260.0,31.0,245.0,"2,778","2,730","2,810","15,081",900.0,"1,491",875,123.0,19.0,80.0,"2,550","2,567","11,631","11,596","11,631",16%,17%,17%
            -,,,,,,,,,,,,,,,,,,,,,,
            -,,,,,,,,,,,,,,,,,,,,,,
        """))
        result = cvo.doutdly_data_cleaner([pd.read_csv(content, header=None, index_col=None)],
                                          'doutdly',
                                          dt.date(2020, 2, 1))
        self.assertEqual(result.head()['Delta Inflow']['Yolo + Misc prev dy'].tolist(),
                         [93.0, 84.0, 76.0, 70.0, 65.0])

    def test_load_pdf_to_dataframe(self):
        """
        test that the load_pdf_to_dataframe processing function works with predictable list of dataframe input that would
        be produced by the tabula-py scraper for a particular date
        """
        content = io.StringIO(textwrap.dedent("""\
            ,,STORAGE,COMPUTED*,SPRING,SHASTA,,EVAP
            ,,ACRE-FEET,INFLOW,CR. P. P.,RELEASE,RELEASE - C.F.S.,C. F. S.
            DAY,ELEV,RES. CHANGE,C.F.S.,RELEASE,C. F. S.,POWER SPILL FISHTRAP,(1)
            ,,"20,616",,,,,
            1,580.93,"20,060 -556","9,900",699,"8,238","9,105 1,070 0",5
            2,582.79,"21,158 1,098","10,650",75,"8,769","9,094 1,002 0",0
            3,582.32,"20,877 -281","9,983",505,"7,740","9,097 1,027 0",1
            4,582.60,"21,044 167","12,108",190,"10,372","9,106 2,914 0",4
            5,582.72,"21,116 72","13,012",655,"10,366","9,252 3,723 0",1
            6,583.14,"21,368 252","13,201",434,"10,769","8,018 5,056 0",0
            7,581.28,"20,264 -1,104","12,414",58,"10,454","6,756 6,215 0",0
            8,582.73,"21,122 858","13,471",85,"10,979","8,808 4,230 0",0
            9,581.47,"20,375 -747","12,711",64,"10,474","8,157 4,931 0",0
            10,582.80,"21,164 789","13,313",61,"11,308","7,250 5,663 0",2
            11,579.96,"19,502 -1,662","12,256",273,"9,919","8,265 4,826 0",3
            12,582.47,"20,967 1,465","13,778",37,"11,382","12,974 60 0",5
            13,580.84,"20,008 -959","12,659",37,"10,389","13,137 0 0",5
            14,581.51,"20,399 391","13,281",37,"10,919","13,078 0 0",6
            15,583.80,"21,769 1,370","13,702",37,"11,282","13,005 0 0",6
            16,585.63,"22,903 1,134","13,628",37,"11,254","13,050 0 0",6
            17,585.42,"22,771 -132","13,030",319,"10,287","13,091 0 0",6
            18,581.17,"20,200 -2,571","11,762",124,"9,417","13,053 0 0",5
            19,582.33,"20,883 683","13,366",669,"10,316","13,017 0 0",5
            20,581.75,"20,540 -343","12,837",47,"10,576","13,003 0 0",7
            21,580.39,"19,749 -791","12,606",115,"10,248","13,000 0 0",5
            22,580.09,"19,576 -173","13,013",540,"10,170","13,094 0 0",6
            23,580.33,"19,714 138","11,344",144,"9,238","11,269 0 0",5
            24,579.42,"19,198 -516","10,750",154,"8,667","11,006 0 0",4
            25,581.64,"20,475 1,277","11,661",446,"9,179","11,011 0 0",6
            26,580.85,"20,014 -461","10,780",111,"8,821","11,008 0 0",4
            27,581.52,"20,405 391","11,191",37,"9,354","10,967 23 0",4
            28,581.68,"20,498 93","10,997",53,"9,210","10,947 0 0",3
            29,582.78,"21,152 654","10,518",37,"8,615","10,168 16 0",4
            30,582.09,"20,740 -412","9,878",355,"7,832","10,081 0 0",5
            31,581.46,"20,370 -370","9,898",105,"7,948","10,080 0 0",5
            TOTALS,,-246,"373,698","6,540","304,492","332,947 40,756 0",118
        """))
        result = cvo.load_pdf_to_dataframe([pd.read_csv(content, header=None, index_col=None)],
                                           dt.date(2023, 5, 1),
                                           'kesdop')
        self.assertEqual(result.tail()['RELEASE - C.F.S.']['POWER'].tolist(),
                         [10967.0, 10947.0, 10168.0, 10081.0, 10080.0])

    def deferred_test_download_files(self):
        """
        this test will eventually be implemented to check the appropriate creation of files from the downloading data
        using
            cvo.download_files(start, end, report_type, destination='.')
        """
        return


if __name__ == '__main__':
    unittest.main()

