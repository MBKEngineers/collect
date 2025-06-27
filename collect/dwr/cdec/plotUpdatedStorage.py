import datetime as dt

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from queries import get_data

from esaconverter import ElevationStorageConverter
converter = ElevationStorageConverter("/Users/sharp/Desktop/hydrology_home/resops/resops/folsom/ratings/elev_stor_area.csv")

station = "FOL"
start = dt.datetime(1995, 1, 1)
end = dt.datetime(2025, 5, 20)

sensors = [76, 23, 15]
sensor_map = {76: "in_cfs", 23: "out_cfs", 15: "sto_af"}


def collect_and_write_data(const_out_cfs, station, start, end, sensors, sensor_map):
    const_out_af = 0
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
    
    # Don't filter dates yet - we need to keep all dates to handle dynamic start dates
    # that might go back before May 15 (e.g., April dates)
    # Filter will be applied later after calculating dynamic start dates

    # create new storage column for updated storage and copy current storage into it
    df_merged["new_sto_af_VALUE"] = df_merged["sto_af_VALUE"].copy()
    df_merged["new_sto_af_UNITS"] = df_merged["sto_af_UNITS"].copy()
    df_merged["max_elevation_VALUE"] = pd.NA  # Initialize as empty 
    df_merged["max_elevation_UNITS"] = pd.NA
    df_merged["const_out_cfs_VALUE"] = pd.NA  # Store year-specific constant outflow in cfs
    df_merged["const_out_af_VALUE"] = pd.NA   # Store year-specific constant outflow in af

    # First pass: find peak storage dates and calculate dynamic start dates
    peak_storage_dates = {}
    dynamic_start_dates = {}
    
    for year in range(start.year, end.year + 1):
        # Get dates for current year (only May 15 - Sept 30 for finding peak)
        may_15 = pd.Timestamp(year, 5, 15)
        sept_30 = pd.Timestamp(year, 9, 30)
        year_data = df_merged[(df_merged.index >= may_15) & 
                             (df_merged.index <= sept_30) & 
                             (df_merged.index.year == year)]
        
        if not year_data.empty:
            # Find peak storage date for this year (within May 15 - Sept 30)
            peak_storage_idx = year_data['sto_af_VALUE'].idxmax()
            peak_storage_dates[year] = peak_storage_idx
            
            # Calculate dynamic start date
            thirty_days_before_peak = peak_storage_idx - pd.DateOffset(days=30)
            year_start_date = min(may_15, thirty_days_before_peak)
            dynamic_start_dates[year] = year_start_date

    # Second pass: calculate constant outflow values using dynamic periods
    for year in range(start.year, end.year + 1):
        if year not in peak_storage_dates or year not in dynamic_start_dates:
            continue
            
        # Get data for the dynamic period (dynamic start date to Sept 30)
        dynamic_start = dynamic_start_dates[year]
        sept_30 = pd.Timestamp(year, 9, 30)
        dynamic_period_data = df_merged[(df_merged.index >= dynamic_start) & 
                                       (df_merged.index <= sept_30)]
        
        if not dynamic_period_data.empty:
            avg_actual_outflow_cfs = dynamic_period_data['out_cfs_VALUE'].mean()

            if avg_actual_outflow_cfs < const_out_cfs:
                year_const_out_cfs = avg_actual_outflow_cfs
                const_out_af = avg_actual_outflow_cfs * 1.983
            else:
                year_const_out_cfs = const_out_cfs
                const_out_af = const_out_cfs * 1.983

            # Store the year-specific constant outflow values for all dates in this year
            df_merged.loc[df_merged.index.year == year, 'const_out_cfs_VALUE'] = year_const_out_cfs
            df_merged.loc[df_merged.index.year == year, 'const_out_af_VALUE'] = const_out_af

    # Third pass: calculate storage with dynamic start dates based on peak storage
    for year in range(start.year, end.year + 1):
        if year not in peak_storage_dates or year not in dynamic_start_dates:
            continue
            
        # Use pre-calculated dynamic start date
        year_start_date = dynamic_start_dates[year]
        
        # Get all dates from dynamic start date through September 30
        year_end_date = pd.Timestamp(year, 9, 30)
        year_dates = df_merged[(df_merged.index >= year_start_date) & 
                              (df_merged.index <= year_end_date)].index.sort_values()
        
        if len(year_dates) == 0:
            continue
            
        const_out_af = df_merged.loc[year_dates[0], 'const_out_af_VALUE']

        max_elev = 0
        max_elev_date = None
        for i in range(len(year_dates) - 1):
            today = year_dates[i]
            tomorrow = year_dates[i+1]

            # Use inflow and constant outflow to calculate storage change
            in_af = df_merged.loc[today, 'in_cfs_VALUE'] * 1.983 #convert in_cfs to in_af
            sto_today = df_merged.loc[today, 'new_sto_af_VALUE']
            
            # Convert current storage to elevation for max check
            elev_today = converter.storage_to_elevation(sto_today) if pd.notnull(sto_today) else 0
            if elev_today > max_elev:
                max_elev = elev_today
                max_elev_date = today

            #update the storage with constant outflow
            if pd.isna(in_af) or in_af == 0:
                sto_tomorrow = sto_today
            else:
                sto_tomorrow = max(0, sto_today + in_af - const_out_af) # prevent sto_tomorrow from being negative
                if sto_tomorrow >= 1250992: sto_tomorrow = 1250992 # prevent storage from exceeding esa curve max value
                
                # Also check if elevation would exceed 484 ft maximum capacity
                elev_tomorrow = converter.storage_to_elevation(sto_tomorrow) if pd.notnull(sto_tomorrow) else 0
                if elev_tomorrow > 484:
                    sto_tomorrow = converter.elevation_to_storage(484) # cap at 484 ft elevation
            
            df_merged.loc[tomorrow, 'new_sto_af_VALUE'] = sto_tomorrow

        # Only set max elevation for the specific date it occurred
        if max_elev_date is not None:
            df_merged.loc[max_elev_date, 'max_elevation_VALUE'] = max_elev
            df_merged.loc[max_elev_date, 'max_elevation_UNITS'] = 'ft'
    
    # Add elevation columns while preserving storage columns
    df_merged['elevation_VALUE'] = df_merged['sto_af_VALUE'].apply(lambda x: converter.storage_to_elevation(x) if pd.notnull(x) else x)
    df_merged['new_elevation_VALUE'] = df_merged['new_sto_af_VALUE'].apply(lambda x: converter.storage_to_elevation(x) if pd.notnull(x) else x)
    df_merged['elevation_UNITS'] = 'ft'
    df_merged['new_elevation_UNITS'] = 'ft'

    # Save the merged DataFrame
    df_merged.to_csv(
        f"/Users/sharp/Desktop/Hydrology_Task_Results/ElevationData/Elevation_{const_out_cfs}.csv",
        index=True,
        index_label="Date",
    )
    print("Data saved.")

def plot_elevation(path):
    df = pd.read_csv(path)
    # Convert Date column to datetime and set as index
    df["Date"] = pd.to_datetime(df["Date"])
    df.set_index("Date", inplace=True)

    # initialize pdf
    with PdfPages(f"/Users/sharp/Desktop/Hydrology_Task_Results/ElevationFlowPlots/Elevation_{const_out_cfs}.pdf") as pdf:
        # Filter data section
        for year in range (start.year, end.year):
            # Find peak storage date for this year (from May 15 - Sept 30)
            may15 = pd.to_datetime(f"{year}-05-15")
            sept30 = pd.to_datetime(f"{year}-09-30")
            year_data = df[(df.index >= may15) & (df.index <= sept30)]
            
            if year_data.empty:
                continue
                
            # Find peak storage date and calculate dynamic start date
            peak_storage_idx = year_data['sto_af_VALUE'].idxmax()
            thirty_days_before_peak = peak_storage_idx - pd.DateOffset(days=30)
            may_15 = pd.to_datetime(f"{year}-05-15")
            dynamic_start_date = min(may_15, thirty_days_before_peak)
            end_date = pd.to_datetime(f"{year}-09-30")
            
            # Now get ALL data for the dynamic range (not just May 15+ data)
            df_filtered = df[(df.index >= dynamic_start_date) & (df.index <= end_date)].copy() 
            df_filtered = df_filtered.interpolate(method="linear")

            # Get the year-specific constant outflow value
            year_const_out_cfs = df_filtered['const_out_cfs_VALUE'].iloc[0] if not df_filtered.empty and pd.notna(df_filtered['const_out_cfs_VALUE'].iloc[0]) else const_out_cfs

            # Create one figure with two stacked subplots
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 9), sharex=True, gridspec_kw={"height_ratios": [2, 1]})

            # Fix min/max elevation calculations
            min_elevation = max(min(df_filtered["elevation_VALUE"].min(),df_filtered["new_elevation_VALUE"].min()), 0) - 5
            max_elevation = max(df_filtered["elevation_VALUE"].max(), df_filtered["new_elevation_VALUE"].max(), 480) + 5
 
            min_flow = 0
            max_flow = 1000 + max(df_filtered['in_cfs_VALUE'].max(), df_filtered['out_cfs_VALUE'].max(), year_const_out_cfs)

            # top plot
            plt.suptitle(f"Folsom Dam & Lake - American River Basin")
            fig.text(0.5, 0.9425, f"Water Year {year} (Start: {dynamic_start_date.strftime('%m/%d')} - Peak: {peak_storage_idx.strftime('%m/%d')})", ha="center", fontsize=10)
            ax1.set_ylabel("elevation (ft)")
            ax1.set_ylim(min_elevation, max_elevation)
            ax1.set_xlim(dynamic_start_date, end_date)  # Set x-axis limits to match data range
            ax1.grid(True)

            # Max elevation point
            max_point_date = df_filtered.loc[df_filtered['max_elevation_VALUE'].notna()].index[0]
            max_point_value = df_filtered.loc[max_point_date, 'max_elevation_VALUE']
            ax1.scatter(
                max_point_date,
                max_point_value,
                color='gold',
                marker='*',
                s=100,
                edgecolor='black',
                linewidth=1,
                label=f'Max Elevation ({round(max_point_value)}ft)',
                zorder=5
            )
            # Add text label below star
            ax1.annotate(
                f'{round(max_point_value)}ft, {max_point_date.strftime("%m/%d")}', 
                xy=(max_point_date, max_point_value),
                xytext=(17, -10),  # 10 points vertically below
                textcoords='offset points',
                ha='center',  # Center text horizontally
                va='top'     # Align top of text with offset point
            )

            # Historical peak storage elevation (actual peak that occurred)
            historical_peak_elev = df_filtered.loc[peak_storage_idx, 'elevation_VALUE']
            ax1.scatter(
                peak_storage_idx,
                historical_peak_elev,
                color='orange',
                marker='o',
                s=80,
                edgecolor='black',
                linewidth=1,
                label=f'Historical Peak ({round(historical_peak_elev)}ft)',
                zorder=5
            )
            # Add text label for historical peak
            ax1.annotate(
                f'{round(historical_peak_elev)}ft, {peak_storage_idx.strftime("%m/%d")}', 
                xy=(peak_storage_idx, historical_peak_elev),
                xytext=(-17, 10),  # 10 points vertically above, offset left
                textcoords='offset points',
                ha='center',
                va='bottom'
            )

            ax1.plot(
                df_filtered.index,
                df_filtered["elevation_VALUE"],
                label="Actual Elevation",
                color="#87CEEA",  
                linewidth=1,
            )

            ax1.plot(
                df_filtered.index,
                df_filtered["new_elevation_VALUE"],  # Changed to plot new elevation
                label=f"Elevation const outflow ({int(year_const_out_cfs)} cfs)",
                color="#1B64F4", 
                linestyle="-",
                linewidth=2,
            )

            ax1.axhline(
                y=466,
                color="r", 
                linestyle="-",
                linewidth=1,
                label="Gross Pool (466ft)",
            )

            ax1.axhline(
                y=471,
                color="r", 
                linestyle="dashed",
                linewidth=1,
                label="Gross Pool +5 ft",
            )

            ax1.axhline(
                y=476,
                color="r", 
                linestyle="dashdot",
                linewidth=1,
                label="Gross Pool +10 ft",
            )

            ax1.legend(loc="center right")

            # bottom plot
            ax2.set_ylabel("flow (cfs)")
            ax2.set_ylim(min_flow, max_flow)
            ax2.grid(True)

            # Format x-axis on the bottom plot
            # Calculate appropriate date range for x-axis ticks
            date_range = (end_date - dynamic_start_date).days
            if date_range <= 60:  # 2 months or less
                ax2.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
                ax2.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
            elif date_range <= 120:  # 4 months or less
                ax2.xaxis.set_major_locator(mdates.MonthLocator())
                ax2.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
            else:  # More than 4 months
                ax2.xaxis.set_major_locator(mdates.MonthLocator())
                ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
            
            # Set x-axis limits to show the full dynamic range
            ax2.set_xlim(dynamic_start_date, end_date)
            plt.xticks(rotation=25)

            ax2.plot(
                df_filtered.index,
                df_filtered["in_cfs_VALUE"],
                label="Inflow",
                color="#39FF14",
                linewidth=1,
            )

            ax2.plot(
                df_filtered.index,
                df_filtered["out_cfs_VALUE"],
                label="Actual Outflow",
                color="#f2c59a", 
                linestyle="-",
                linewidth=1,
            )

            ax2.axhline(
                y=year_const_out_cfs,
                color="#fc8613",  
                linestyle="-",
                linewidth=2,
                label=f"Constant Outflow ({int(year_const_out_cfs)} cfs)",
            )

            ax2.legend(loc="center right")

            plt.tight_layout(rect=[0, 0, 1, .98])
            pdf.savefig(fig)
            plt.close(fig)

const_out_cfs = 5000

collect_and_write_data(const_out_cfs, station, start, end, sensors, sensor_map) 
plot_elevation(f"/Users/sharp/Desktop/Hydrology_Task_Results/ElevationData/Elevation_{const_out_cfs}.csv")
