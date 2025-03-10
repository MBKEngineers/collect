import datetime as dt
import pandas as pd 

import io
import re
import requests
import ssl


def get_url(datetime_structure):

	now = dt.datetime.now(tz=datetime_structure.tzinfo)
	days = (now-datetime_structure).days

	return f'https://www.spk-wc.usace.army.mil/fcgi-bin/midnight.py?days={days-1}&report=FCR&textonly=true'	

def get_sac_valley_fcr(datetime_structure):

	url = get_url(datetime_structure)
	content = requests.get(url, verify=ssl.CERT_NONE).text
	return re.findall(r'(?<=Sacramento Valley)[\S\s]*(?=San Joaquin Valley)', content)

#Extract first section of Sacramento Valley data
def extract_sacvalley(datetime_structure):
	"""
    get the full FCR report page from url and store the data into a pandas dataframe

    Arguments:
        content (str): the full FCR report page
    Returns:
        df (pandas.DataFrame): the USACE station storage and flood control parameters data, specific to Sacramento valley
    """
	query = get_sac_valley_fcr(datetime_structure)
	query2 = (query[0].split('-------------- --------- --------- --------- -------- -------------- ---------------\n')[1])
	query3 = query2.split('Folsom')[0].replace('CFS','')
	df = pd.read_fwf(io.StringIO(query3), header=None)
	df[5] = df[5].str.replace('(','')
	df[6] = df[6].str.replace(')','')
	df.columns = ["Reservoir", "Gross Pool (acft)", "Top of Conservation (acft)","Actual Res (acft)", r"% of Gross Pool", "Above top of Conservation (acft)","% Encroached", "Flood Control Parameters (Rain in.)","Flood Control Parameters (Snow acft)"]
	df= df.set_index("Reservoir")
	return df

# Extract second section of Sacramento Valley data 
def extract_folsom(datetime_structure):
	"""
    get the full FCR report page from url and store the data into a pandas dataframe

    Arguments:
        content (str): the full FCR report page
    
    Returns:
        result_cleaned (pandas.DataFrame): the USACE station storage and flood control parameters, specific to Folsom
    """
	query4 = get_sac_valley_fcr(datetime_structure)
	query5 = (query4[0].split('Indian Valley:')[1])
	query6 = (query5.split('BASIN TOTALS')[0]).replace("-","").replace("(","").replace(")","").replace(";","").replace(",","")

	pattern = r'(-\d{1,3},\d*)'
	nval = re.findall(pattern,query5)
	dfnval = pd.DataFrame(nval, columns= ['negative_num1'])
	# print(dfnval)

	df2 = pd.read_fwf(io.StringIO(query6), names =["sc"])
	df2[['Forecasted Date','Forecasted_Time','Top of Conservation (acft)','Actual Res (acft)',r'% of GrossPool','Above Top of Conservation(acft)', 'Percent Encroached']]= df2['sc'].str.extract(r'(\d{2}[A-Z]{3}\d{4}) (\b\d+\b)              (\b\d+\b)   (\b\d+\b)  (\b\d+\b)       (\b\d+\b)  (\b\d+\b)') #works

	#replace existing values from Above Top of Conservation(acft) column with the negative values and thousandths place

	for i in range(len(dfnval)):
		row_n = df2.index == i+2
		df2.loc[row_n,'Above Top of Conservation(acft)'] = dfnval.loc[i,'negative_num1']

	#Use row index and column number to add extracted values to dataframe
	row_index = df2.index == 0
	df2.loc[row_index, ['Forecasted Date','Ex1.1']] = df2.loc[row_index,'sc'].str.extract(r'([A-Za-z]+):          (-+?\d+)').values

	# new_row_index1 = df2.index==4
	# df2.loc[new_row_index1, ['Forecasted Date']] = df2.loc[new_row_index1,'sc'].str.extract(r'([A-Za-z]+ [A-Za-z]+)').values

	new_row_index2 = df2.index==5
	df2.loc[new_row_index2, ['Forecasted Date']] = df2.loc[new_row_index2,'sc'].str.extract(r'([A-Za-z]+ [A-Za-z]+)').values

	df_post_drop = df2.drop('sc',axis=1)
	df_post_drop = df_post_drop[["Forecasted Date","Forecasted_Time","Ex1.1",'Top of Conservation (acft)','Actual Res (acft)',r'% of GrossPool','Above Top of Conservation(acft)', 'Percent Encroached']]
	
	#Forecasted section
	query41 = query4
	query51 = query5
	query61 = query6
	dfVol = pd.read_fwf(io.StringIO(query61), names= ["scratch"])
	dfVol[['Forecasted Volumes****','fv1','fv2','fv3']] = dfVol['scratch'].str.extract(r'(?:\s*(\d+)\s+)?(?:\s*(\d+)\s+)?(\d+)\s+(\d+)$')

	rowindex = dfVol.index == 1
	dfVol.loc[rowindex,['Reservoir','Gross Pool (acft)']] = dfVol.loc[rowindex,'scratch'].str.extract(r'([a-zA-Z]+):          (\d+)').values

	df_post_drop2 = dfVol.drop('scratch',axis=1)
	df_post_drop2 = df_post_drop2[["Reservoir","Gross Pool (acft)","Forecasted Volumes****","fv1","fv2","fv3"]]

	#concenate dataframe along rows
	result = pd.concat([df_post_drop2,df_post_drop], axis=1)

	result_reordered = result.loc[:,['Reservoir','Gross Pool (acft)','Top of Conservation (acft)','Actual Res (acft)', r'% of GrossPool','Above Top of Conservation(acft)','Percent Encroached','Forecasted Date','Forecasted_Time','Forecasted Volumes****','fv1','fv2','fv3']]
	result_cleaned = result_reordered.dropna(how='all')
	#fill na's with Reservoir name and Gross pool (acft)
	rc = result_cleaned.iloc[:,[0,1]].fillna(method='pad')

	#Replacement DataFrame (same index)
	df_replace = pd.DataFrame(rc, index=result_cleaned.index)
	# Replace the first column
	result_cleaned.iloc[:, :1]= df_replace
	#Drop the first row
	result_cleaned = result_cleaned.drop(result_cleaned.index[0])
	result_cleaned = result_cleaned.set_index("Reservoir")
	return result_cleaned

#Extract third section of Sacramento Valley data 
def extract_basintotals(datetime_structure):
	"""
    get the full FCR report page from url and store the data into a pandas dataframe

    Arguments:
        content (str): the full FCR report page
    Returns:
        df_after_dropping (pandas.DataFrame): the USACE station basin total storage data, total flood space encroached and w/US storages included
    """
	qy = get_sac_valley_fcr(datetime_structure)
	qy1 = (qy[0].split(' ____________________________________________________________________________________')[1])
	qy2 = qy1.split(' **  Percent Encroached')[0]
	df3 = pd.read_fwf(io.StringIO(qy2), names= ["Col0","Actual Res (acft)",r'% of GrossPool',"Above Top of Conservation(acft)","Percent Encroached"]) #read fixed-width format file into pandas Dataframe
	df3["Above Top of Conservation(acft)"] = df3["Above Top of Conservation(acft)"].str.replace("(", "")
	df3["Percent Encroached"] = df3["Percent Encroached"].str.replace(")", "") 
	df3[['Reservoir','Gross Pool (acft)','Top of Conservation (acft)']] = df3["Col0"].str.extract(r'([a-zA-Z]+ [a-zA-Z]+)  (\d+,\d+,\d+) (\d+,\d+,\d+)')
	df3.loc[1,'Reservoir'] = df3.loc[1,'Col0'] 
	rowindexer = df3.index == 2
	df3.loc[rowindexer,['Reservoir','Gross Pool (acft)']] = df3.loc[rowindexer,'Col0'].str.extract(r'([a-zA-Z]+[a-zA-Z]+ [a-zA-Z]+) (\d+,\d+,\d+)').values
	print(df3.loc[rowindexer,'Col0'].str.extract(r'([a-zA-Z]+[a-zA-Z]+ [a-zA-Z]+) (\d+,\d+,\d+)').values)

	df_after_dropping = df3.drop('Col0',axis=1)
	df_after_dropping = df_after_dropping[["Reservoir", "Gross Pool (acft)","Top of Conservation (acft)","Actual Res (acft)",r'% of GrossPool',"Above Top of Conservation(acft)","Percent Encroached"]]
	df_after_dropping = df_after_dropping.set_index("Reservoir")
	return df_after_dropping


if __name__ == '__main__':

	datetime_structure = dt.datetime(2025,2,25)
	df = extract_sacvalley(datetime_structure)
	result_cleaned = extract_folsom(datetime_structure)
	df_after_dropping = extract_basintotals(datetime_structure)

	merge_df = pd.concat([df,result_cleaned],axis =0)
	merge_df2 = pd.concat([merge_df,df_after_dropping],axis=0)
	# merge_df2.to_excel("result.xlsx")


	#Add notes.txt file 
	qylin = re.findall(r'[\S\s]*(?= ---- Sacramento Valley ----)', content)
	df4 = pd.read_fwf(io.StringIO(qylin)) #read fixed-width format file into pandas Dataframe
