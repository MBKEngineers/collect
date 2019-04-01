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


WILDEYE_URL = 'https://app.mywildeye.com/Login'
MBK_USER = os.environ['WILDEYE_USER']
MBK_PASSWORD = str(os.environ['WILDEYE_PASSWORD'])
SAMPLE_SITE_IDS = '116082__636852122101335523', '122982__636852124416805473'

