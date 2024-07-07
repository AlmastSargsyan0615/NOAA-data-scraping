import time
import pandas as pd
from noaastn import noaastn
from datetime import datetime


weather_data = noaastn.get_weather_data("911650-22536", 2020)
print(weather_data)
time.sleep(100000)

# Get current date and time
current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Get station data
stationData_US = noaastn.get_stations_info(country="US")

# Convert the station data to a DataFrame
station_df = pd.DataFrame(stationData_US)

# Construct the filename with current date and time
filename = f"station_data_{current_datetime}.csv"

# Write the DataFrame to a CSV file
station_df.to_csv(filename, index=False)

print(f"Data written to {filename}")
