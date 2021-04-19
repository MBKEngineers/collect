from collect.dwr.b120 import *


for date in ['_20180316', '_20150316']:
    # df = get_b120_data(date_suffix=date)
    # print(df)

    df = get_b120_update_data(date_suffix=date) # only goes back to 2018 in this format, maybe just ditch
    print(df)
    df = get_120_update_data_new_format(2021, 3)
    # df = get_120_archived_reports(2018, 3)
    print(df)