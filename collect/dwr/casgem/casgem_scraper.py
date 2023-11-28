"""
collect.dwr.casgem.casgem_scraper
============================================================
access CASGEM well data
"""
# -*- coding: utf-8 -*-
from io import StringIO
import os
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def get_casgem_data(casgem_id=None,
                    state_well_number=None,
                    local_well_designation=None,
                    master_site_code=None,
                    write_to_html_file=False):
    """
    Download well timeseries data from CASGEM database; return as dataframe

    search term                 | type  |  example
    ----------------------------------------------------------------------
        casgem_id               |  str  |  '34318'
        state_well_number       |  str  |  '19N02W36H001M'
        local_well_designation  |  str  |  '19N02W36H001M'
        master_site_code        |  str  |  '394564N1220246W001'

    Args:
        casgem_id (str): desc
        state_well_number (None): desc
        local_well_designation (None): desc
        master_site_code (None): desc
        write_to_html_file (bool): desc
    
    Returns:
        dict
    """
    # if os.name == 'posix':
    #     chromedriver = '/usr/local/bin/chromedriver'
    # elif os.name == 'windows':
    #     # update Chromedriver to 2.36 (latest on Win32)
    #     chromedriver = 'C:/Python27/Scripts/chromedriver'
    # os.environ['webdriver.chrome.driver'] = chromedriver
    # chrome_options = Options()
    # chrome_options.add_argument('--dns-prefetch-disable')
    driver = webdriver.Chrome()

    # fetch log in url
    url = 'https://www.casgem.water.ca.gov'
    driver.get(url)

    url = driver.current_url.split('?ReturnUrl')[0]
    casgem_session = url.split('(S(')[-1].split(')')[0]
    driver.get(url)

    # add username to login form
    user_field = driver.find_element_by_id('txtUserName')
    user_field.send_keys(os.environ['CASGEM_USER'])

    # add password to login form
    password_field = driver.find_element_by_id('txtPassword')
    password_field.send_keys(os.environ['CASGEM_PASSWORD'])

    # click submit on login form
    driver.find_element_by_name('btnLogin').click()

    # fetch well search URL
    search_url = url.replace('default.aspx', 'Public/SearchWells.aspx')
    # sometime case varies...
    search_url = search_url.replace('Default.aspx', 'Public/SearchWells.aspx')

    # may need to repeat if search URL doesn't load at first
    driver.get(search_url)

    # fields for basic well search
    local_well_designation_field = driver.find_element_by_id('ctl00_CASGEMBody_txtWellDesignation')
    casgem_well_number_field = driver.find_element_by_id('ctl00_CASGEMBody_txtCASGEMWellNumber')
    casgem_well_id_field = driver.find_element_by_id('ctl00_CASGEMBody_txtCASGEMWellId')
    state_well_number_field = driver.find_element_by_id('ctl00_CASGEMBody_txtStateWellNumber')

    # search by CASGEM well ID
    if casgem_id is not None:
        casgem_well_id_field.send_keys(str(casgem_id))

    # search by local well designation
    elif local_well_designation is not None:
        local_well_designation_field.send_keys(str(local_well_designation))

    # search by master site code (CASGEM well number)
    elif master_site_code is not None:
        casgem_well_number_field.send_keys(str(master_site_code))

    # search by state well number
    elif state_well_number is not None:
        state_well_number_field.send_keys(str(state_well_number))
    
    # submit query
    driver.find_element_by_name('ctl00$CASGEMBody$btnSearch').click()
    driver.find_element_by_id('ctl00_CASGEMBody_ucWells_grdWellList_ctl00_ctl04_lnkViewDetails').click()

    # switch to well info window
    aw = driver.window_handles
    driver.switch_to_window(aw[1])

    # grab geography/elevation data
    geography = {
        'longitude': float(WebDriverWait(driver, 100).until(
            EC.presence_of_element_located((By.ID, 'ctl00_CASGEMBody_lblLongitude'))
        ).text),
        'latitude': float(driver.find_element_by_id('ctl00_CASGEMBody_lblLatitude').text),
        'elevation_rp': float(driver.find_element_by_id('ctl00_CASGEMBody_tdRPElevation').text.replace('ft.','')),
        'elevation_ground_surface': float(driver.find_element_by_id('ctl00_CASGEMBody_tdGSElevation').text.replace('ft.','')),
    }

    try:
        geography.update({
            'accuracy': float(driver.find_element_by_id('ctl00_CASGEMBody_lblAccuracy').text.replace('ft.','')),
            'well_use': driver.find_element_by_id('ctl00_CASGEMBody_lblWellUse').text,
            'well_status': driver.find_element_by_id('ctl00_CASGEMBody_lblWellStatus').text,
        })
    except:
        pass

    # find tabular well elevation data
    element = driver.find_element_by_id('ctl00_CASGEMBody_lnkElevationData')
    element.click()

    page_length_element = driver.find_element_by_id('ctl00_CASGEMBody_grdWellElevation_ctl00_ctl03_ctl01_PageSizeComboBox_Input')
    for li in [5, 20, 50, 100, 'All']:
        page_length_element.send_keys(Keys.DOWN)
    page_length_element.send_keys(Keys.RETURN)

    table_element = driver.find_element_by_id('ctl00_CASGEMBody_grdWellElevation_ctl00')

    # write to file-like object
    html_file_content = StringIO()
    html_file_content.write(table_element.get_attribute('outerHTML'))
    html_file_content.seek(0)

    if write_to_html_file:
        try:
            # Python 2.7
            with open('{}_casgem_data.html'.format(casgem_id), 'wb') as html_file:
                html_file.write(table_element.get_attribute('outerHTML'))

            html_file_content = open('{}_casgem_data.html'.format(casgem_id), 'rb')
        except TypeError:
            # Python 3
            with open('{}_casgem_data.html'.format(casgem_id), 'w') as html_file:
                html_file.write(table_element.get_attribute('outerHTML'))

            html_file_content = open('{}_casgem_data.html'.format(casgem_id), 'r')

    wait = WebDriverWait(driver, 100)

    driver.close()
    driver.quit()

    # parse HTML file structure; extract tabular data
    soup = BeautifulSoup(html_file_content, 'lxml')
    table = soup.find('table')

    # extract (visible) column headers
    columns = []
    thead = table.find_all('thead')[0]
    for th in thead.find_all('th', {"class": "rgHeader"}):
        if 'style' in th.attrs.keys():
            if not ('display:none;' in th.attrs['style']):
                columns.append(th.text)

    # (visible) table data
    table_list = []
    for tr in table.find_all('tbody')[-1].find_all('tr'):
        row = []
        for td in tr.find_all('td'):
            if ('style' not in td.attrs.keys()) or ('display:none;' not in td.attrs['style']):
                text = str(td.text.strip())
                try:
                    row.append(float(text))
                except ValueError:
                    if text in ['', '&nbsp']:
                        row.append(float('nan'))
                    else:
                        row.append(text)
        table_list.append(row)

    # construct DataFrame
    df = pd.DataFrame.from_records(table_list, coerce_float=True, columns=columns)

    # timezone conversion
    df.index = pd.to_datetime(df['Date'] + ' ' + df['Military Time (PST)'])
    df.index = df.index.tz_localize('US/Pacific')

    return { 'data': df, 'geography': geography }
