from collect.dwr.b120 import *


for date in ['_20210316', '_20180316', '_20150316']:
    df = get_b120_data(date_suffix=date)
    print(df)

# df = get_b120_update_data(date_suffix='_201904') # only goes back to 2018 in this format, maybe just ditch
# df = get_120_archived_reports(2018, 3)
