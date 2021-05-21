"""
collect.dwr.delta_conditions
============================================================
access DWR Delta Conditions PDF
"""
# -*- coding: utf-8 -*-
import datetime as dt
import camelot

import io
import re
from bs4 import BeautifulSoup
import pandas as pd
import requests


def df_cleanup(df):
    nan_value = float("NaN")
    df.replace("", nan_value, inplace=True)
    # df = df.dropna(axis=1)
    df = df.replace(',','', regex=True)
    df = df.replace('\n','', regex=True)
    return df

if __name__ == "__main__":

    pdf_path = 'https://water.ca.gov/-/media/DWR-Website/Web-Pages/Programs/State-Water-Project/Operations-And-Maintenance/Files/Operations-Control-Office/Delta-Status-And-Operations/Delta-Operations-Daily-Summary.pdf'

    tables = camelot.read_pdf(pdf_path, flavor='stream',  strip_text=' .\n')

    # Read the first page and clean data
    first = tables[0].df
    first = first.drop(first.index[0:3], axis=0)
    print(first)

    first = df_cleanup(first)
    print(first)

    # print(first.to_csv())
