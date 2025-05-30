import datetime as dt
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages

# Load the existing data
df = pd.read_csv("/Users/sharp/Desktop/Hydrology_Task/HydrologyTastData/dataOneTry.csv")

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

# Initialize PDF
with PdfPages("/Users/sharp/Desktop/Hydrology_Task/HydrologyTastData/plots.pdf") as pdf:
    # Filter data section
    for year in range(1995, 2025):
        start_date = pd.to_datetime(f"{year}-05-01")
        end_date = pd.to_datetime(f"{year}-10-01")
        df_filtered = df[(df.index >= start_date) & (df.index <= end_date)].copy()

        # Linearly interpolate any missing values
        df_filtered = df_filtered.interpolate(method="linear")

        min_storage = (
            df_filtered["Storage"].min() if "Storage" in df_filtered.columns else 0
        )
        min_flow = 0
        max_flow = max(df_filtered["Inflow AF"].max(), df_filtered["Outflow AF"].max())
        max_flow += 1000
        if max_flow < 9915:
            max_flow = 12000

        # Create figure with subplots
        fig, (ax1, ax2) = plt.subplots(
            2, 1, figsize=(12, 9), sharex=True, gridspec_kw={"height_ratios": [2, 1]}
        )

        # Storage plot
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
                linestyle="--",
                linewidth=2,
            )

        plt.suptitle(f"Folsom Dam & Lake - American River Basin")
        fig.text(0.5, 0.9425, f"Water Year {year}", ha="center", fontsize=10)

        # Set y-axis limits for storage plot
        max_y = max(900000, df_filtered["Storage w Constant Outflow AF"].max())
        ax1.set_ylim(min_storage - 50000, max_y + 100000)
        ax1.set_ylabel("storage (ac-ft)")
        ax1.axhline(
            y=966000,
            color="#318DAF",
            linestyle="-",
            linewidth=1,
            label="Gross Pool (966k)",
        )
        ax1.legend(loc="center right")
        ax1.grid(True)

        # Flow plot
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
            y=9915,
            color="r",
            linestyle="--",
            linewidth=2,
            label="Constant Outflow (9915)",
        )
        ax2.legend(loc="center right")
        ax2.grid(True)

        # Format x-axis
        ax2.xaxis.set_major_locator(mdates.MonthLocator())
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        plt.xticks(rotation=45)

        plt.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)

print("PDF regenerated successfully.")
