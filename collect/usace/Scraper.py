
import pandas as pd 
import dateutil.parser
import re
import io
import requests
# from bs4 import BeautifulSoup
import urllib3

# df = pd.read_csv(io.StringIO(page.decode('utf-8')), header=0)

url = f'https://www.spk-wc.usace.army.mil/fcgi-bin/midnight.py?days=0&report=FCR&textonly=true'
response = requests.get(url, verify=False)
urllib3.disable_warnings()
content = response.text

# print(content.split('Sacramento Valley')[1].split('San Joaquin Valley')[0])

#Extract first section of Sacramento Valley data

query  = re.findall(r'(?<=Sacramento Valley)[\S\s]*(?=San Joaquin Valley)', content)
query2 = (query[0].split('-------------- --------- --------- --------- -------- -------------- ---------------\n')[1])
query3 = query2.split('Folsom')[0]
df = pd.read_fwf(io.StringIO(query3), header=None)
df[5] = df[5].str.replace('(','')
df[6] = df[6].str.replace(')','')
df.columns = ["Reservoir", "Gross Pool (acft)", "Top of Conservation (acft)","Actual Res (acft)", r"% of Gross Pool", "Above top of Conservation (acft)","% Encroached", "Flood Control Parameters (Rain in.)","Flood Control Parameters (Snow acft)"]
df= df.set_index("Reservoir")
print(df)
df.to_excel("Scraper.xlsx")
print(df)



# Extract second section of Sacramento Valley data 
query4 = re.findall(r'(?<=Sacramento Valley)[\S\s]*(?=San Joaquin Valley)', content)
query5 = (query4[0].split('Indian Valley:')[1])
query6 = (query5.split('BASIN TOTALS')[0]).replace("-","").replace("(","").replace(")","").replace(";","").replace(",","")
df2 = pd.read_fwf(io.StringIO(query6), names =["sc"])
print(df2)

df2[['Reservoir','Ex2','Top of Conservation (acft)','Actual Res (acft)',r'% of GrossPool','Above Top of Conservation(acft)', 'Percent Encroached']]= df2['sc'].str.extract(r'(\d{2}[A-Z]{3}\d{4}) (\b\d+\b)              (\b\d+\b)   (\b\d+\b)  (\b\d+\b)       (\b\d+\b)  (\b\d+\b)') #works
print(df2)
row_index = df2.index == 0
df2.loc[row_index, ['Reservoir','Ex1.1']] = df2.loc[row_index,'sc'].str.extract(r'([A-Za-z]+):          (\d+)').values

# new_row_index = df2.index==3
# df2.loc[new_row_index, ['Ex1']] = df2.loc[new_row_index,'Reservoir'].str.extract(r'([A-Za-z]+ [A-Za-z]+)').values

new_row_index1 = df2.index==4
df2.loc[new_row_index1, ['Reservoir']] = df2.loc[new_row_index1,'sc'].str.extract(r'([A-Za-z]+ [A-Za-z]+)').values

new_row_index2 = df2.index==5
df2.loc[new_row_index2, ['Reservoir']] = df2.loc[new_row_index2,'sc'].str.extract(r'([A-Za-z]+ [A-Za-z]+)').values
# print(df2['Ex1'])

df_post_drop = df2.drop('sc',axis=1)
df_post_drop = df_post_drop[["Reservoir", "Ex1.1",'Top of Conservation (acft)','Actual Res (acft)',r'% of GrossPool','Above Top of Conservation(acft)', 'Percent Encroached']]
print(df_post_drop)

df_post_drop.to_excel("Scraper2.xlsx")

#Forecasted section
query41 = re.findall(r'(?<=Sacramento Valley)[\S\s]*(?=San Joaquin Valley)', content)
query51 = (query41[0].split(' Indian Valley:   300,600      -NR-      -NR-               -NR-(  0)  ----      ---')[1])
query61 = (query51.split('BASIN TOTALS')[0]).replace("-","").replace("(","").replace(")","").replace(";","").replace(",","")
dfVol = pd.read_fwf(io.StringIO(query61), names= ["scratch"])
dfVol[['Forecasted Volumes****','fv1','fv2','fv3']] = dfVol['scratch'].str.extract(r'(?:\s*(\d+)\s+)?(?:\s*(\d+)\s+)?(\d+)\s+(\d+)$')
rowindex = dfVol.index == 0
dfVol.loc[rowindex,['Reservoir','GrossPool']] = dfVol.loc[rowindex,'scratch'].str.extract(r'([a-zA-Z]+):          (\d+)').values
df_post_drop2 = dfVol.drop('scratch',axis=1)
df_post_drop2 = df_post_drop2[["Reservoir","GrossPool","Forecasted Volumes****","fv1","fv2","fv3"]]
print(df_post_drop2)

# # dfVol['']= df2['Reservoir'].str.extract(r'(\b\d+\b);    (\b\d+\b);    (\b\d+\b);   (\b\d+\b)') #works
df_post_drop2.to_excel("example.xlsx")
# print(df2)


# #Extract third section of Sacramento Valley data 
qy = re.findall(r'(?<=Sacramento Valley)[\S\s]*(?=San Joaquin Valley)', content)
qy1 = (qy[0].split(' ____________________________________________________________________________________')[1])
qy2 = qy1.split(' **  Percent Encroached')[0]
df3 = pd.read_fwf(io.StringIO(qy2), names= ["Col0","Col1", "Col2","Col3","Col4"]) #read fixed-width format file into pandas Dataframe
df3["Col3"] = df3["Col3"].str.replace("(", "")
df3["Col4"] = df3["Col4"].str.replace(")", "") 

df3[['letters','numbers','Num']] = df3["Col0"].str.extract(r'([a-zA-Z]+ [a-zA-Z]+)  (\d+,\d+,\d+) (\d+,\d+,\d+)')
df3.loc[1,'letters'] = df3.loc[1,'Col0'] 
rowindexer = df3.index == 2
df3.loc[rowindexer,['letters','numbers']] = df3.loc[rowindexer,'Col0'].str.extract(r'([a-zA-Z]+[a-zA-Z]+ [a-zA-Z]+) (\d+,\d+,\d+)').values
print(df3.loc[rowindexer,'Col0'].str.extract(r'([a-zA-Z]+[a-zA-Z]+ [a-zA-Z]+) (\d+,\d+,\d+)').values)

df_after_dropping = df3.drop('Col0',axis=1)
df_after_dropping = df_after_dropping[["letters", "numbers","Num","Col1","Col2","Col3","Col4"]]
print(df_after_dropping)

