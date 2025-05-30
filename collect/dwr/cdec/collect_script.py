import datetime as dt
from queries import get_data

result = get_data("FOL", dt.datetime(1994, 1, 1), dt.datetime(2025, 5, 20), 76, "D")


# Store the DataFrame as a CSV file
result['data'].to_csv('FOL_inflow76.csv', index=True)

print("Data saved.'")