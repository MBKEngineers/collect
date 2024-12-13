
import pandas as pd 
import dateutil.parser
import re
import io
import requests
# from bs4 import BeautifulSoup
import urllib3

# grosspool = []
# rain = []
# snow = []

url = f'https://www.spk-wc.usace.army.mil/fcgi-bin/midnight.py?days=0&report=FCR&textonly=true'
response = requests.get(url, verify=False)
urllib3.disable_warnings()
content = response.text


# df = pd.read_csv(io.StringIO(page.decode('utf-8')), header=0)
# print(content.split('Sacramento Valley')[1].split('San Joaquin Valley')[0])

##Extract first section of Sacramento Valley data

# query  = re.findall(r'(?<=Sacramento Valley)[\S\s]*(?=San Joaquin Valley)', content)
# query2 = (query[0].split('-------------- --------- --------- --------- -------- -------------- ---------------\n')[1])
# query3 = query2.split('Folsom')[0]
# df = pd.read_fwf(io.StringIO(query3), header=None)
# df[5] = df[5].str.replace('(','')
# df[6] = df[6].str.replace(')','')
# df.columns = ["Reservoir", "Gross Pool (acft)", "Top of Conservation (acft)","Actual Res (acft)", r"% of Gross Pool", "Above top of Conservation (acft)","% Encroached", "Flood Control Parameters (Rain in.)","Flood Control Parameters (Snow acft)"]

# df= df.set_index("Reservoir")
# df.to_excel("Scraper.xlsx")
# print(df)


##Extract second section of Sacramento Valley data 
# query4 = re.findall(r'(?<=Sacramento Valley)[\S\s]*(?=San Joaquin Valley)', content)
# query5 = (query4[0].split(' ---\n Indian Valley:   300,600   260,600   234,800  78        -25,800(  0)  ----')[1])
# query6 = query5.split(' **  Percent Encroached')[0]
# df2 = pd.read_fwf(io.StringIO(query6), header=None)
# print(df2)


# #Extract third section of Sacramento Valley data 
# qy = re.findall(r'(?<=Sacramento Valley)[\S\s]*(?=San Joaquin Valley)', content)
# qy1 = (qy[0].split(' ____________________________________________________________________________________')[1])
# qy2 = qy1.split(' **  Percent Encroached')[0]
# df3 = pd.read_fwf(io.StringIO(qy2), header=None)
# # print(df3)
# dfex = df3[0]
# split_data = dfex.str.rsplit(n=2)
# print(split_data)


# df3[['ex1','ex2']] = df3[0].str.split(' ', n=1, expand=True)
# print(df3)
# df3 =pd.df3(df3.row.str.split(' ',n=1, expand=True).tolist(),columns=['Basin Tot','Total Flood Space Encroached','w/US Storages'])
