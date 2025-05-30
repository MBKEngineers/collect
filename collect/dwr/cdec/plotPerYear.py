import datetime as dt

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from queries import get_data

from esaconverter import ElevationStorageConverter
print("debug 1")
# Expects value in cfs
OUTFLOW_CFS = 5000
OUTFLOW_AF = OUTFLOW_CFS * 1.983

station = "FOL"
start = dt.datetime(1994, 1, 1)
end = dt.datetime(2025, 5, 20)

sensors = [76, 23, 15]
sensor_map = {76: "Inflow", 23: "Outflow", 15: "Storage"}

converter = ElevationStorageConverter("/Users/sharp/Desktop/hydrology_home/resops/resops/folsom/ratings/elev_stor_area.csv")

# load all "VALUE" and "UNITS" data from each sensor
dfs = []
for sensor in sensors:
    result = get_data(station, start, end, sensor, "D")
    df = result["data"].copy()
    df = df[["VALUE", "UNITS"]]

    # Convert VALUE column to numeric, coerce any errors to NaN
    df["VALUE"] = pd.to_numeric(df["VALUE"], errors="coerce")

    df.columns = [f"{sensor_map[sensor]}_{col}" for col in df.columns]
    dfs.append(df)

# merge all data frames into one 
df_merged = pd.concat(dfs, axis=1)

# Convert index to datetime directly (no need for DATE TIME column)
df_merged.index = pd.to_datetime(df_merged.index)
df_merged["Storage_with_Constant_Outflow"] = df_merged["Storage_VALUE"].copy()
df_merged["Storage_with_Constant_Outflow_UNITS"] = df_merged["Storage_UNITS"].copy()
print("debug 2")
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
        current_date = dates[i]
        next_date = dates[i + 1]

        # Use inflow and constant outflow to calculate storage change
        inflow = df_merged.loc[current_date, "Inflow_VALUE"] * 1.983
        current_storage = df_merged.loc[current_date, "Storage_with_Constant_Outflow"]

        # Calculate new storage
        if pd.isna(inflow) or inflow == 0:
            new_storage = df_merged.loc[current_date, "Storage_with_Constant_Outflow"]
        else:
            new_storage = max(0, current_storage + inflow - OUTFLOW_AF)
            if new_storage >= 1250992: new_storage = 1250992

        df_merged.loc[next_date, "Storage_with_Constant_Outflow"] = new_storage
print("debug 3")
# Convert any storage value to elevation (acre-ft to ft)
mask = (df_merged["Storage_UNITS"] == 'AF') | ((df_merged["Storage_with_Constant_Outflow_UNITS"] == 'AF'))
print("debug 4")
df_merged.loc[mask, "Storage_VALUE"] = df_merged.loc[mask, "Storage_VALUE"].apply(
    lambda x: converter.storage_to_elevation(x) if pd.notnull(x) else x
)
print("debug 5")
df_merged.loc[mask, "Storage_with_Constant_Outflow"] = df_merged.loc[mask, "Storage_with_Constant_Outflow"].apply(
    lambda x: converter.storage_to_elevation(x) if pd.notnull(x) else x
)
print("debug 6")
df_merged.loc[mask, "Storage_UNITS"] = 'FT'
df_merged.loc[mask, "Storage_with_Constant_Outflow_UNITS"] = 'FT'
print("debug 7")

# Save the merged DataFrame with index as a named column (moved outside the year loop)
df_merged.to_csv(
    f"/Users/sharp/Desktop/Hydrology_Task/HydrologyTastData/data_const_outflow_{OUTFLOW_CFS}.csv",
    index=True,
    index_label="Date",
)

print("Data saved.")









# Load the data and set the index (moved outside the year loop)
df = pd.read_csv(f"/Users/sharp/Desktop/Hydrology_Task/HydrologyTastData/data_const_outflow_{OUTFLOW_CFS}.csv")

# Debug: print column names
print("Available columns:", df.columns.tolist())

# Convert Date column to datetime and set as index
df["Date"] = pd.to_datetime(df["Date"])
df.set_index("Date", inplace=True)

# Update column names to match plotting code
df = df.rename(
    columns={
        "Storage_VALUE": "Storage",
        "Inflow_VALUE": "Inflow AF",
        "Outflow_VALUE": "Outflow AF",
        "Storage_with_Constant_Outflow": "Storage w Constant Outflow AF",
    }
)

# Initialize PDF before the plotting loop
with PdfPages(
    f"/Users/sharp/Desktop/Hydrology_Task/HydrologyTastData/plots_{OUTFLOW_CFS}.pdf"
) as pdf:
    # Filter data section
    for year in range(1995, 2025):
        start_date = pd.to_datetime(f"{year}-05-01")
        end_date = pd.to_datetime(f"{year}-10-01")
        df_filtered = df[(df.index >= start_date) & (df.index <= end_date)].copy()

        # Linearly interpolate any missing values in numeric columns
        df_filtered = df_filtered.interpolate(method="linear")

        min_storage = (
            df_filtered["Storage"].min() if "Storage" in df_filtered.columns else 0
        )
        min_flow = 0
        max_flow = (
            df_filtered["Inflow AF"].max()
            if df_filtered["Inflow AF"].max() > df_filtered["Outflow AF"].max()
            else df_filtered["Outflow AF"].max()
        )
        max_flow += 1000
        if max_flow < 9915:
            max_flow = 12000

        # Create one figure with two stacked subplots
        fig, (ax1, ax2) = plt.subplots(
            2, 1, figsize=(12, 9), sharex=True, gridspec_kw={"height_ratios": [2, 1]}
        )

        # --- First plot (full height) ---
        if "Storage" in df.columns:
            ax1.plot(
                df_filtered.index,
                df_filtered["Storage"],
                label="Storage",
                color="blue",
                linewidth=1,
            )
        if "Storage w Constant Outflow AF" in df.columns:
            ax1.plot(
                df_filtered.index,
                df_filtered["Storage w Constant Outflow AF"],
                label="Storage w Constant Outflow",
                color="r",
                linestyle="-",
                linewidth=1,
            )

        plt.suptitle(f"Folsom Dam & Lake - American River Basin")
        fig.text(0.5, 0.9425, f"Water Year {year}", ha="center", fontsize=10)
        ax1.set_ylabel("storage (ac-ft)")
        # Calculate max storage value and set upper limit
        max_y = max(500, df_filtered["Storage w Constant Outflow AF"].max())
        min_y = min(
            min_storage - 10,
            df_filtered["Storage w Constant Outflow AF"].min() - 10,
        )
        ax1.set_ylim(min_y, max_y + 50)
        ax1.axhline(
            y=466,
            color="#318DAF",
            linestyle="-",
            linewidth=3,
            label="Gross Pool (ENTER)",
        )
        ax1.legend(loc="center right")
        ax1.grid(True)

        # --- Second plot (shorter height) ---
        if "Inflow AF" in df.columns:
            ax2.plot(
                df_filtered.index,
                df_filtered["Inflow AF"],
                label="Inflow",
                color="g",
                linewidth=1,
            )
        if "Outflow AF" in df.columns:
            ax2.plot(
                df_filtered.index,
                df_filtered["Outflow AF"],
                label="Outflow",
                color="#FF8000",
                linewidth=1,
            )

        ax2.set_ylabel("flow (ac-ft)")
        ax2.set_ylim(min_flow, max_flow)
        ax2.axhline(
            y=OUTFLOW_AF,
            color="r",
            linestyle="--",
            linewidth=2,
            label=f"Constant Outflow ({OUTFLOW_AF})",
        )
        ax2.legend(loc="center right")
        ax2.grid(True)

        # Format x-axis on the bottom plot
        ax2.xaxis.set_major_locator(mdates.MonthLocator())
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        plt.xticks(rotation=45)

        # Adjust layout and save
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)
# PDF will automatically close due to 'with' statement
