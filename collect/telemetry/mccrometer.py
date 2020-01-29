# -*- coding: utf-8 -*-
import os
import time
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import dateutil.parser
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait


# load credentials
load_dotenv()


def mccrometer_scraper(panel_id):
    """
    Download timeseries data from USGS database; return as dataframe
    ---------------------|---------------|----------------------------
    argument             | type          |  example
    ---------------------|---------------|----------------------------
        panel_id         |  int or str   |  9610
    ---------------------|---------------|----------------------------

    """
    username = os.environ.get('MCCROMETER_USER')
    password = os.environ.get('MCCROMETER_PASSWORD')

    # define chrome driver executable
    driver = webdriver.Chrome(executable_path=os.environ.get('CHROMEDRIVER'))

    # get login page
    driver.get('http://fc01.mccrometer.net')

    user_field = driver.find_element_by_name('j_username')
    user_field.send_keys(username)

    # add password
    password_field = driver.find_element_by_name('j_password')
    password_field.send_keys(password)

    # wait
    wait = WebDriverWait(driver, 2000)

    # submit login form
    try:
        driver.find_element_by_id('login').click()
    except:
        driver.find_element_by_id('loginAnyhow').click()

    # wait
    wait = WebDriverWait(driver, 2000)

    # access site-data url (try twice, because lag prevents driver from noticing)
    time_id = int(round(time.time() * 1000))
    url = '&'.join(['http://fc01.mccrometer.net/secure/trend/values?startTime=0', 
                   'endTime={time_id}', 
                   'panel={panel}']).format(time_id=time_id, panel=panel_id)

    # currently automatically downloads the CSV data to downloads folder
    driver.get(url)

    # close window
    driver.quit()

    # read data to dataframe from buffer
    return {'data': None, 'info': {'panel': panel_id}}
