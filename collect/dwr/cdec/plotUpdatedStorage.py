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

# Single variable to control days before peak for both data collection and plotting
DAYS_BEFORE_PEAK = 0

sensors = [76, 23, 15]
sensor_map = {76: "in_cfs", 23: "out_cfs", 15: "sto_af"}

startday = 15
startmonth = 5

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
    
    # Clean up zero values by forward-filling with previous day's values
    # Replace zeros with NaN first, then forward fill
    value_columns = ['in_cfs_VALUE', 'out_cfs_VALUE', 'sto_af_VALUE']
    for col in value_columns:
        if col in df_merged.columns:
            # Replace zeros with NaN
            df_merged[col] = df_merged[col].replace(0, pd.NA)
            # Forward fill to carry previous day's value
            df_merged[col] = df_merged[col].fillna(method='ffill')
    
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
    df_merged["updated_out_cfs_VALUE"] = pd.NA  # Store daily updated outflow in cfs

    # Apply dynamic outflow rule to ALL dates, not just dynamic period
    for index, row in df_merged.iterrows():
        actual_out_cfs = row['out_cfs_VALUE']
        df_merged.loc[index, 'updated_out_cfs_VALUE'] = const_out_cfs
        
        if actual_out_cfs <= const_out_cfs and pd.notna(actual_out_cfs):
            df_merged.loc[index, 'updated_out_cfs_VALUE'] = actual_out_cfs

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
            # thirty_days_before_peak = peak_storage_idx - pd.DateOffset(days=DAYS_BEFORE_PEAK) 
            year_start_date = min(may_15, pd.Timestamp(year, startmonth, startday))
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

    # Third pass: calculate storage for entire water year, starting from dynamic start date
    for year in range(start.year, end.year + 1):
        if year not in peak_storage_dates or year not in dynamic_start_dates:
            continue
            
        # Use pre-calculated dynamic start date
        year_start_date = dynamic_start_dates[year]
        
        # Get all dates from dynamic start date through September 30 of the NEXT year
        # This ensures we capture the full water year effect
        sept_30 = pd.Timestamp(year, 9, 30)
        year_dates = df_merged[(df_merged.index >= year_start_date) & 
                              (df_merged.index <= sept_30)].index.sort_values()
        
        if len(year_dates) == 0:
            continue
            
        const_out_af = df_merged.loc[year_dates[0], 'const_out_af_VALUE']

        max_elev = 0
        max_elev_date = None
        reached_484_date = None  # Track when 484 ft is reached
        
        # Find the first date with valid storage data to use as baseline
        baseline_storage = None
        baseline_date = None
        for date in year_dates:
            if pd.notna(df_merged.loc[date, 'sto_af_VALUE']):
                baseline_storage = df_merged.loc[date, 'sto_af_VALUE']
                baseline_date = date
                break
        
        if baseline_storage is None:
            continue  # Skip this year if no valid storage data found
        
        # Initialize storage for all dates up to and including baseline date
        for i, date in enumerate(year_dates):
            if date <= baseline_date:
                df_merged.loc[date, 'new_sto_af_VALUE'] = baseline_storage
            else:
                break
        
        # Process each day sequentially starting from the day after baseline
        baseline_idx = list(year_dates).index(baseline_date)
        for i in range(baseline_idx + 1, len(year_dates)):
            today = year_dates[i]
            yesterday = year_dates[i-1]

            # Check if we should stop (5 days after reaching 484 ft)
            if reached_484_date is not None:
                days_since_484 = (today - reached_484_date).days
                if days_since_484 > 5:
                    break  # Stop processing this year

            # Get today's inflow and actual outflow
            in_af = df_merged.loc[today, 'in_cfs_VALUE'] * 1.983 #convert in_cfs to in_af
            actual_out_cfs = df_merged.loc[today, 'out_cfs_VALUE']
            
            # Dynamic daily outflow rule: if actual >= const_out_cfs, use const_out_cfs; otherwise use actual
            if actual_out_cfs >= const_out_cfs:
                daily_out_cfs = const_out_cfs
            else:
                daily_out_cfs = actual_out_cfs
            
            # Store the updated outflow value
            df_merged.loc[today, 'updated_out_cfs_VALUE'] = daily_out_cfs
            
            # Get yesterday's UPDATED storage
            sto_yesterday = df_merged.loc[yesterday, 'new_sto_af_VALUE']
            
            daily_out_af = daily_out_cfs * 1.983  # convert to acre-feet
            
            #update the storage with dynamic daily outflow
            if pd.isna(in_af) or in_af == 0:
                # If no inflow data or zero inflow, keep storage the same
                sto_today = sto_yesterday
            else:
                # Calculate new storage: previous UPDATED storage + inflow - outflow
                sto_today = sto_yesterday + in_af - daily_out_af
                
                # Ensure storage doesn't go negative
                sto_today = max(0, sto_today)
                
                # Cap storage at maximum capacity
                if sto_today >= 1250992: 
                    sto_today = 1250992
                
                # Also check if elevation would exceed 484 ft maximum capacity
                elev_today = converter.storage_to_elevation(sto_today) if pd.notnull(sto_today) else 0
                if elev_today > 484:
                    sto_today = converter.elevation_to_storage(484) # cap at 484 ft elevation
            
            # Store today's updated storage
            df_merged.loc[today, 'new_sto_af_VALUE'] = sto_today
            
            # Convert current storage to elevation for max check
            elev_today = converter.storage_to_elevation(sto_today) if pd.notnull(sto_today) else 0
            
            # Check if we've reached 484 ft for the first time
            if elev_today >= 484 and reached_484_date is None:
                reached_484_date = today
            
            if elev_today > max_elev:
                max_elev = elev_today
                max_elev_date = today

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

    # Check if we have any data to plot to avoid empty PDF warning
    has_data_to_plot = False
    for year in range(start.year, end.year):
        year_data = df[df.index.year == year]
        if not year_data.empty:
            has_data_to_plot = True
            break
    
    if not has_data_to_plot:
        print("No data to plot.")
        return

    # initialize pdf
    with PdfPages(f"/Users/sharp/Desktop/Hydrology_Task_Results/ElevationFlowPlots/Elevation_{const_out_cfs}.pdf") as pdf:
        # Filter data section
        for year in range (start.year, end.year):
            # Get all data for this year to ensure we have the full range available
            year_data = df[df.index.year == year]
            
            if year_data.empty:
                continue
            
            # Find peak storage date for this year (from May 15 - Sept 30 only)
            may15 = pd.to_datetime(f"{year}-05-15")
            sept30 = pd.to_datetime(f"{year}-09-30")
            peak_data = year_data[(year_data.index >= may15) & (year_data.index <= sept30)]
            
            if peak_data.empty:
                continue
                
            # Find peak storage date and calculate dynamic start date
            peak_storage_idx = peak_data['sto_af_VALUE'].idxmax()
            dynamic_start_date = pd.to_datetime(f"{year}-0{startmonth}-{startday}")
            
            # Find where updated storage reaches 484 ft and set end date 5 days after
            reached_484_data = year_data[year_data['new_elevation_VALUE'] >= 484]
            if not reached_484_data.empty:
                reached_484_date = reached_484_data.index[0]
                end_date = reached_484_date + pd.DateOffset(days=5)
                # Don't go past September 30
                sept_30_limit = pd.to_datetime(f"{year}-09-30")
                end_date = min(end_date, sept_30_limit)
            else:
                end_date = pd.to_datetime(f"{year}-09-30")
            
            # Now get ALL data for the dynamic range (not just May 15+ data)
            df_filtered = df[(df.index >= dynamic_start_date) & (df.index <= end_date)].copy() 
            df_filtered = df_filtered.interpolate(method="linear")

            # Skip if no data in filtered range
            if df_filtered.empty:
                continue

            # Get the year-specific constant outflow value
            year_const_out_cfs = df_filtered['const_out_cfs_VALUE'].iloc[0] if not df_filtered.empty and pd.notna(df_filtered['const_out_cfs_VALUE'].iloc[0]) else const_out_cfs

            # Get historical peak elevation from original data (not filtered)
            historical_peak_elev = year_data.loc[peak_storage_idx, 'elevation_VALUE']

            # Create one figure with two stacked subplots
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 9), sharex=True, gridspec_kw={"height_ratios": [2, 1]})

            # Fix min/max elevation calculations
            min_elevation = max(min(df_filtered["elevation_VALUE"].min(),df_filtered["new_elevation_VALUE"].min()), 0) - 5
            max_elevation = max(df_filtered["elevation_VALUE"].max(), df_filtered["new_elevation_VALUE"].max(), 480) + 5
 
            min_flow = 0
            max_flow = 1000 + max(df_filtered['in_cfs_VALUE'].max(), df_filtered['out_cfs_VALUE'].max(), year_const_out_cfs)

            # top plot
            plt.suptitle(f"Folsom Dam & Lake - American River Basin")
            fig.text(0.5, 0.9425, f"Water Year {year} (Start: {dynamic_start_date.strftime('%m/%d')} - Historical Peak: {peak_storage_idx.strftime('%m/%d')} ({round(historical_peak_elev)}ft))", ha="center", fontsize=10)
            ax1.set_ylabel("elevation (ft)")
            ax1.set_ylim(min_elevation, max_elevation)
            ax1.set_xlim(dynamic_start_date, end_date)  # Set x-axis limits to match data range
            ax1.grid(True)

            # Max elevation point - check if max elevation data exists
            max_elev_data = df_filtered.loc[df_filtered['max_elevation_VALUE'].notna()]
            if not max_elev_data.empty:
                max_point_date = max_elev_data.index[0]
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
            else:
                # If no max elevation data in the filtered range, find max from new_elevation_VALUE
                max_new_elev_idx = df_filtered['new_elevation_VALUE'].idxmax()
                max_new_elev_value = df_filtered.loc[max_new_elev_idx, 'new_elevation_VALUE']
                ax1.scatter(
                    max_new_elev_idx,
                    max_new_elev_value,
                    color='gold',
                    marker='*',
                    s=100,
                    edgecolor='black',
                    linewidth=1,
                    label=f'Max Elevation ({round(max_new_elev_value)}ft)',
                    zorder=5
                )
                # Add text label below star
                ax1.annotate(
                    f'{round(max_new_elev_value)}ft, {max_new_elev_idx.strftime("%m/%d")}', 
                    xy=(max_new_elev_idx, max_new_elev_value),
                    xytext=(17, -10),  # 10 points vertically below
                    textcoords='offset points',
                    ha='center',  # Center text horizontally
                    va='top'     # Align top of text with offset point
                )

            # Historical peak storage elevation (actual peak that occurred)
            if peak_storage_idx in df_filtered.index:
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

            # Find where updated elevation crosses 466 ft (gross pool) using interpolation
            crosses_466_data = df_filtered[df_filtered['new_elevation_VALUE'] >= 466]
            if not crosses_466_data.empty:
                # Find the first crossing point
                crossing_idx = crosses_466_data.index[0]
                
                # Get the previous point for interpolation
                crossing_position = df_filtered.index.get_loc(crossing_idx)
                if crossing_position > 0:
                    prev_idx = df_filtered.index[crossing_position - 1]
                    prev_elev = df_filtered.loc[prev_idx, 'new_elevation_VALUE']
                    curr_elev = df_filtered.loc[crossing_idx, 'new_elevation_VALUE']
                    
                    # Linear interpolation to find exact 466 ft crossing
                    if prev_elev < 466 <= curr_elev:
                        # Calculate the fraction between the two points where 466 occurs
                        fraction = (466 - prev_elev) / (curr_elev - prev_elev)
                        # Interpolate the date
                        time_diff = crossing_idx - prev_idx
                        crosses_466_date = prev_idx + fraction * time_diff
                    else:
                        crosses_466_date = crossing_idx
                else:
                    crosses_466_date = crossing_idx
                
                ax1.scatter(
                    crosses_466_date,
                    466,  # Use exactly 466 ft
                    color='red',
                    marker='s',
                    s=80,
                    edgecolor='black',
                    linewidth=1,
                    label=f'Crosses Gross Pool ({crosses_466_date.strftime("%m/%d")})',
                    zorder=5
                )
                # Add text label for crosses gross pool point
                ax1.annotate(
                    f'{crosses_466_date.strftime("%m/%d")}', 
                    xy=(crosses_466_date, 466),
                    xytext=(10, 15),  # 15 points vertically above, offset right
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

            ax2.plot(
                df_filtered.index,
                df_filtered["updated_out_cfs_VALUE"],
                color="#fc8613",  
                linestyle="-",
                linewidth=2,
                label=f"Updated Outflow (max {const_out_cfs} cfs)",
            )

            ax2.legend(loc="center right")

            plt.tight_layout(rect=[0, 0, 1, .98])
            pdf.savefig(fig)
            plt.close(fig)

const_out_cfs = 5000

collect_and_write_data(const_out_cfs, station, start, end, sensors, sensor_map) 
plot_elevation(f"/Users/sharp/Desktop/Hydrology_Task_Results/ElevationData/Elevation_{const_out_cfs}.csv")
collect_and_write_data(const_out_cfs, station, start, end, sensors, sensor_map) 
plot_elevation(f"/Users/sharp/Desktop/Hydrology_Task_Results/ElevationData/Elevation_{const_out_cfs}.csv")
