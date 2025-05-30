import datetime as dt

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from queries import get_data

from esaconverter import ElevationStorageConverter
converter = ElevationStorageConverter("/Users/sharp/Desktop/hydrology_home/resops/resops/folsom/ratings/elev_stor_area.csv")

const_out_cfs = 5000
const_out_af = 5000 * 1.983
station = "FOL"
start = dt.datetime(1994, 1, 1)
end = dt.datetime(2025, 5, 20)

sensors = [76, 23, 15]
sensor_map = {76: "in_cfs", 23: "out_cfs", 15: "sto_af"}


def collect_and_write_data(const_out_af, station, start, end, sensors, sensor_map):
    dfs = []
    for sensor in sensors:
        result = get_data(station, start, end, sensor, "D")
        df = result["data"].copy()

        #get only the units and value from each batch of sensor data
        df = df[["VALUE", "UNITS"]]

        # ex: df.columns = [in_cfs_VALUE, in_cfs_UNITS]
        df.columns = [f"{sensor_map[sensor]}_{col}" for col in df.columns]
        dfs.append(df)

    # merge all data frames into one --> ex: inflow, outflow, storage all in one df
    df_merged = pd.concat(dfs, axis=1)

    # Convert index to datetime directly (no need for DATE TIME column)
    df_merged.index = pd.to_datetime(df_merged.index)

    # create new storage column for updated storage and copy current storage into it
    df_merged["new_sto"] = df_merged["sto_af_VALUE"].copy()
    df_merged["new_sto_units"] = df_merged["sto_af_UNITS"].copy()

    for year in range(start.year, end.year + 1):
        # Get mask for May 1 to Oct 1 for current year
        mask = (
            (df_merged.index.month == 5) & (df_merged.index.day >= 1)
            | (df_merged.index.month > 5) & (df_merged.index.month < 10)
            | (df_merged.index.month == 10) & (df_merged.index.day == 1)
        ) & (df_merged.index.year == year)

        # Get the dates where we need to calculate
        dates = df_merged.index[mask]
        
        for i in range(len(dates) - 1):
            today = dates[i]
            tomorrow = dates[i+1]

            # Use inflow and constant outflow to calculate storage change
            in_af = df_merged.loc[today, 'in_cfs_VALUE'] * 1.983 #convert in_cfs to in_af
            sto_today = df_merged.loc[today, 'new_sto_af']

            #update the storage with constant outflow
            if pd.isna(in_af) or in_af == 0:
                sto_tomorrow = sto_today
            else:
                sto_tomorrow = min(0, sto_today + in_af - const_out_af) # prevent sto_tomorrow from being negative
                if sto_tomorrow >= 1250992: sto_tomorrow = 1250992
            
            df_merged.loc(tomorrow, 'new_sto_af') = sto_tomorrow

    # Save the merged DataFrame with index as a named column (moved outside the year loop)
    df_merged.to_csv(
        f"/Users/sharp/Desktop/Hydrology_Task/HydrologyTastData/data_const_outflow_{const_out_cfs}_k.csv",
        index=True,
        index_label="Date",
    )

    print("Data saved.")
