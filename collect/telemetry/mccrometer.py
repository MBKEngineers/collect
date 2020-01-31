# -*- coding: utf-8 -*-
import os
import time
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import dateutil.parser
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# load credentials
load_dotenv()

PANEL_IDS = [int(x) for x in os.environ.get('MCCROMETER_ALL_PANELS')]
FLOW_PANELS = [int(x) for x in os.environ.get('MCCROMETER_FLOW_PANELS')]


def download_panel_csv(panel_id, download_path=None):
    """
    Download timeseries data from USGS database; return as dataframe
    ---------------------|---------------|----------------------------
    argument             | type          |  example
    ---------------------|---------------|----------------------------
        panel_id         |  int or str   |  9610
    ---------------------|---------------|----------------------------

    """
    options = webdriver.ChromeOptions() 
    # options.set_headless(True)

    if download_path is None:
        download_path = os.environ.get('MCCROMETER_DOWNLOAD_PATH')

    file_path = os.path.join(download_path, 'McCrometer', str(panel_id))
    try:
        os.makedirs(file_path)
    except:
        pass

    options.add_experimental_option("prefs", {"download.default_directory" : file_path})

        # define chrome driver executable
    driver = webdriver.Chrome(executable_path=os.environ.get('CHROMEDRIVER'), 
                              chrome_options=options)

    login(driver)

        # access site-data url (try twice, because lag prevents driver from noticing)
    time_id = int(round(time.time() * 1000))
    url = '&'.join(['http://fc01.mccrometer.net/secure/trend/values?time={time_id}', 
                   'panel={panel}']).format(time_id=time_id, panel=panel_id)

    # currently automatically downloads the CSV data to downloads folder
    wait = WebDriverWait(driver, 10000)
    driver.get(url)

    # close window
    # try:
    #     element = WebDriverWait(driver, 10).until(
    #         EC.presence_of_element_located((By.TAG_NAME, "table"))
    #     )
    # finally:
    #     driver.quit()

    while not os.path.exists(os.path.join(file_path, 'values.csv')):
        while os.path.getsize(os.path.join(file_path, 'values.csv')) < 50000:
            pass
    driver.quit()

    # read data to dataframe from buffer
    return {'data': None, 'info': {'panel': panel_id}}


def login(driver):
    """
    """
    # get login page
    driver.get('http://fc01.mccrometer.net')

    # login credentials
    username = os.environ.get('MCCROMETER_USER')
    password = os.environ.get('MCCROMETER_PASSWORD')

    user_field = driver.find_element_by_name('j_username')
    user_field.send_keys(username)

    # add password
    password_field = driver.find_element_by_name('j_password')
    password_field.send_keys(password)

    # wait
    driver.implicitly_wait(10)
    wait = WebDriverWait(driver, 10000)

    # submit login form
    try:
        driver.find_element_by_id('login').click()
    except:
        driver.find_element_by_id('loginAnyhow').click()

    # wait
    driver.get('http://fc01.mccrometer.net')

    try:
        element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
    finally:
        return
        # driver.quit()
