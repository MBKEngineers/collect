"""
collect.dwr.delta_conditions
============================================================
access DWR Delta Conditions PDF
"""
# -*- coding: utf-8 -*-
import datetime as dt

import io
import re
from bs4 import BeautifulSoup
import pandas as pd
import requests
from pdfminer.high_level import extract_text


def df_cleanup(df):
    nan_value = float("NaN")
    df.replace("", nan_value, inplace=True)
    # df = df.dropna(axis=1)
    df = df.replace(',','', regex=True)
    df = df.replace('\n','', regex=True)
    return df

if __name__ == "__main__":

    url = 'https://water.ca.gov/-/media/DWR-Website/Web-Pages/Programs/State-Water-Project/Operations-And-Maintenance/Files/Operations-Control-Office/Delta-Status-And-Operations/Delta-Operations-Daily-Summary.pdf'

    response = requests.get(url)
    result = extract_text(io.BytesIO(response.content))

    # # in-memory file buffer
    # with io.StringIO(result) as buf:

    #     # parse fixed-width text-formatted table
    #     df = pd.read_fwf(buf, 
    #                      header=[0], 
    #                      skiprows=[0, 1, 2, 3, 4, 5], 
    #                      na_values=['<i>Missing</i>', 'Missing']
    #                      )
    #     # df = pd.read_table(buf)

    # # print(df)
    # df.columns = ['Column']

    # row = 'Clifton Court Inflow  ='

    # inflow = df.loc[df['Column']==row]
    # print(inflow)

    import PyPDF2
    # Creating a pdf file object
    pdfFileObj = open(result,'rb')
    # Creating a pdf reader object
    pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
    # Getting number of pages in pdf file
    pages = pdfReader.numPages
    # Loop for reading all the Pages
    for i in range(pages):
        # Creating a page object
        pageObj = pdfReader.getPage(i)
        # Printing Page Number
        print("Page No: ",i)
        # Extracting text from page
        # And splitting it into chunks of lines
        text = pageObj.extractText().split("  ")
        # Finally the lines are stored into list
        # For iterating over list a loop is used
        for i in range(len(text)):
                # Printing the line
                # Lines are seprated using "\n"
                print(text[i],end="\n\n")
        # For Seprating the Pages
        print()
    # closing the pdf file object
    pdfFileObj.close()


    # tables = camelot.read_pdf(url, flavor='stream',  strip_text=' .\n')

    # # Read the first page and clean data
    # first = tables[0].df
    # first = first.drop(first.index[0:3], axis=0)
    # print(first)

    # first = df_cleanup(first)
    # print(first)

    # # print(first.to_csv())
