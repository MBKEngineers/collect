# -*- coding: utf-8 -*-
import datetime as dt
import os
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import pandas as pd
from pytz import timezone
import requests


class WildeyeOutpost(object):
    """
    opdata:site entry
    """

    def __init__(self, name=None, site_id=None, project_reference=None, latitude=None, longitude=None, firmware=None):

        self.name = name
        self.site_id = site_id
        self.project_reference = project_reference
        self.latitude = latitude
        self.longitude = longitude
        self.firmware = firmware
        self.logger = None
        self.inputs = []


class WildeyeLogger(object):
    """
    opdata:logger entry
    """

    def __init__(self, last_telemetry=None, logger_id=None, make=None, model=None, imsi=None, sim=None):

        self.last_telemetry = last_telemetry
        self.logger_id = logger_id
        self.make = make
        self.model = model
        self.imsi = imsi
        self.sim = sim


class WildeyeInput(object):
    """
    opdata:input entry
    """

    def __init__(self, name=None, input_id=None, scada_tag_id=None, log_interval=None, unit=None):
        self.name = name
        self.input_id = input_id
        self.scada_tag_id = scada_tag_id
        self.log_interval = log_interval
        self.unit = unit
        self.records = []

    def records_as_dataframe(self):
        df = pd.DataFrame(self.records)
        df['date'] = df['date'].apply(parse_wildeye_date)
        return df.set_index('date')


def get_query_url(start_date, end_date, outpostID=None):
    """
    start_date and end_date are datetime objects
    optional outpostID argument limits data records to only one sensor apparatus | op38608, op38609

    generates URL of format
    h​ttps://www.outpostcentral.com/api/2.0/meterservice/mydata.aspx?userName=[YourUserName]
    &password=[YourPassword]&dateRead=1/Sep/2009%2000:00:00
    """

    # date format must preserve HTML empty space as %20 between date and time stamps
    date_format = '%-d/%b/%Y%%20%H:%M:%S'

    # url parameters for dataservice REST API
    url_arguments = [
        'https://www.outpostcentral.com/api/2.0/dataservice/mydata.aspx?',
        'userName={user}',
        'password={password}',
        'dateFrom={start_date:%-d/%b/%Y%%20%H:%M:%S}',
        'dateTo={end_date:%-d/%b/%Y%%20%H:%M:%S}',
    ]

    # optional additional parameter
    if outpostID:
        url_arguments.append('outpostID={opID}'.format(opID=outpostID))

    # load credentials
    load_dotenv()

    # join url parts
    return '&'.join(url_arguments).format(user=os.getenv('WILDEYE_USER'),
                                          password=str(os.getenv('WILDEYE_PASSWORD')),
                                          start_date=start_date,
                                          end_date=end_date)


def parse_wildeye_date(x):
    return dt.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S')


def scrape_data(url):

    # PACIFIC = timezone('America/Los_Angeles')

    soup = BeautifulSoup(requests.get(url).content, 'lxml')

    sites = soup.find('opdata:data').find('opdata:sites').find_all('opdata:site')

    collection = []

    for site in sites:

        outpost = WildeyeOutpost(name=site.find('name').text,
                                 site_id=site.find('id').text,
                                 latitude=site.find('latitude').text,
                                 longitude=site.find('longitude').text)

        try:
            outpost.project_reference = site.find('projectReference').text
        except:
            pass
        try:
            outpost.firmware = site.find('firmware').text
        except:
            pass

        for logger in site.find('opdata:loggers').find_all('opdata:logger'):

            outpost.logger = WildeyeLogger(last_telemetry=logger.find('last_telemetry'),
                                           logger_id=logger.find('id').text,
                                           make=logger.find('make').text,
                                           model=logger.find('model').text,
                                           imsi=logger.find('imsi').text,
                                           sim=logger.find('sim').text)

        for input_ in site.find('opdata:inputs').find_all('opdata:input'):

            logger_input = WildeyeInput(name=input_.find('name').text,
                             input_id=input_.find('id').text,
                             unit=input_.find('unit').text)

            try:
                logger_input.scada_tag_id = input_.find('scadaTagID').text
            except:
                pass
            try:
                logger_input.log_interval = input_.find('logInterval').text
            except:
                pass

            for record in input_.find('opdata:records').find_all('opdata:record'):

                logger_input.records.append({'date': record.find('date').text, 'value': record.find('value').text})

            outpost.inputs.append(logger_input)

        collection.append(outpost)

    return collection


if __name__ == '__main__':

    result = scrape_data(get_query_url(dt.datetime(2018, 10, 2), 
                                       dt.datetime(2018, 10, 12), 
                                       outpostID='op38608'))

    for y in result:
        for x in y.inputs:
            try:
                print(x.records_as_dataframe().head())
            except:
                print(x.name)