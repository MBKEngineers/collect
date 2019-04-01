# -*- coding: utf-8 -*-

# Python Standard Lib Imports
import datetime
import os
from StringIO import StringIO

# Third Party Python Imports
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait


class MeterData(object):
    program = None
    device = None
    serialno = None
    ident = None
    version = None
    upload_interval = None
    channels = None
    interval = None
    points = None
    
    company = 'Mace'
    url = None
    data = None

    def read_site_characteristics_from_html(self, html_buffer):
        
        # define tree for extracting site characteristics
        soup = BeautifulSoup(html_buffer, 'lxml')
        data = soup.find('div', {'class': 'site-data'})

        # extract info from table cells with column span of 7
        for x in data.findAll('td', {'colspan': '7'}):
            row = x.text[1:].replace('"','').split(',')
            setattr(self, row[0].lower().replace(' ','_'), ','.join(row[1:]))


def mace_scraper(url):
    """
    Store your login info in environment variables.
    You'll need to download the Selenium Chrome Driver that works 
    with your system and assign it to an environment variable too.
    """
    username = os.environ.get('TELEMETRY_USERNAME')
    password = os.environ.get('TELEMETRY_PASSWORD_MACE')

    # define chrome driver executable
    driver = webdriver.Chrome(executable_path=os.environ.get('CHROMEDRIVER'))

    # get login page
    driver.get('https://macemeters.com/support/users/profile')

    # add user name
    user_field = driver.find_element_by_id('login-email')
    user_field.send_keys(username)

    # add password
    password_field = driver.find_element_by_id('login-password')
    password_field.send_keys(password)

    # submit login form
    driver.find_element_by_name('submitButton').click()

    # wait
    wait = WebDriverWait(driver, 10000)

    # access site-data url (try twice, because lag prevents driver from noticing)
    driver.get(url)
    driver.get(url)

    # select div containing site data table
    page_info = driver.find_element_by_class_name('site-data')
    
    # write data to buffer
    html_data = StringIO()
    html_data.write(page_info.get_attribute('outerHTML'))
    html_data.seek(0)

    # close window
    driver.quit()

    # read data to dataframe from buffer
    return read_site_data_from_html(html_data)


def read_site_data_from_html(html_buffer):
    """
    HTML data from Mace Meter page takes form:
    <div class="site-data"><table><tbody><tr><td>....data rows....</td></tr></tbod></table></div>
    """
    # read timeseries data from buffer, accounting for top 10 rows, which contain other info
    frame = pd.read_html(html_buffer, header=[7, 8], skiprows=[9], encoding='utf-8')[0][:-1]
    header = ['{0} ({1})'.format(x, y) if not 'Unnamed' in y else x for (x, y) in frame.columns.tolist()]
    frame.columns = ['Date/Time'] + header[1:]

    # convert time data column to dataframe index (no timezone info applied yet)
    date_parser = lambda x: datetime.datetime.strptime(x, '%Y/%m/%d %H:%M:%S')
    frame.index = frame['Date/Time'].replace({r'[^\x00-\x7F]+':' '}, regex=True).apply(date_parser)
    frame.drop('Date/Time', axis=1, inplace=True)

    return frame


if __name__ == '__main__':

    m = MeterData()
    m.url = '&'.join([
        'https://macemeters.com/webcomm/sites/show/00891/?suppress-status-messages=on',
        'start-date=2017-11-01',
        'end-date=2017-12-01',
        'submitButton=View+site+data'
    ])
    m.data = mace_scraper(m.url)
