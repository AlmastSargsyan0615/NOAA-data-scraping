from geopy.distance import geodesic
import json
import time
import os
import requests
import pandas as pd
import csv
from collections import Counter

def get_keys(json_data):
    if isinstance(json_data, dict):
        return json_data.keys()
    return []

def download_csv(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            filename = os.path.basename(url)
            with open(filename, "wb") as f:
                f.write(response.content)
            print("File downloaded successfully.")
            return filename
        else:
            print("Failed to download the file. Status code:", response.status_code)
            return None
    except Exception as e:
        print("An error occurred:", e)
        return None

def remove_files(file_paths):
    for file_path in file_paths:
        try:
            os.remove(file_path)
            print(f"File {file_path} removed successfully.")
        except Exception as e:
            print(f"An error occurred while removing {file_path}: {e}")

def get_county_info(file_path, fips):
    data = pd.read_csv(file_path)
    print(f"CSV columns: {data.columns.tolist()}")
    
    data['FIPS'] = data['FIPS'].astype(str)
    
    filtered_data = data[data['FIPS'] == fips]
    print(f"Filtered data: {filtered_data}")
    
    if not filtered_data.empty:
        county_name = filtered_data['County_Name'].values[0]
        state = filtered_data['State'].values[0]
        latitude = filtered_data['Latitude'].values[0]
        longitude = filtered_data['Longitude'].values[0]
        return county_name, state, latitude, longitude
    else:
        return None, None, None, None

def get_avg_and_nearest_station_data(file_paths, year, month, day, hour, county_centroid):
    year = int(year)
    month = int(month)
    day = int(day)
    hour = int(hour)
    
    date_str = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}"
    winds = []
    temperatures = []
    temperatures_qualities = []
    precipitations = []
    
    distances = []

    nearest_station_id = None
    nearest_station_distance = float('inf')
    nearest_station_data = {}

    for file_path in file_paths:
        station_id = file_path.split('.')[0]
        with open(file_path, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            headers = reader.fieldnames
            has_lat = 'LATITUDE' in headers
            has_lon = 'LONGITUDE' in headers
            has_wnd = 'WND' in headers
            has_tmp = 'TMP' in headers
            has_aa1 = 'AA1' in headers

            for row in reader:
                if date_str in row['DATE']:
                    station_lat = float(row['LATITUDE']) if has_lat and row['LATITUDE'] and row['LATITUDE'] != "999" else None
                    station_lon = float(row['LONGITUDE']) if has_lon and row['LONGITUDE'] and row['LONGITUDE'] != "999" else None
                    
                    if station_lat and station_lon:
                        distance_to_centroid = geodesic((station_lat, station_lon), county_centroid).miles
                        distances.append(distance_to_centroid)

                        if distance_to_centroid < nearest_station_distance:
                            nearest_station_distance = distance_to_centroid
                            nearest_station_id = station_id
                            nearest_station_data = {
                                "wind": None,
                                "temp": None,
                                "temp_quality": None,
                                "precip": None
                            }

                    if has_wnd and row['WND'] and row['WND'] != "999,9,9,9999,9":
                        wnd_parts = row['WND'].split(',')
                        if len(wnd_parts) >= 4:
                            wind_speed = float(wnd_parts[3])
                            winds.append(wind_speed)
                            if nearest_station_id == station_id:
                                nearest_station_data["wind"] = wind_speed

                    if has_tmp and row['TMP'] and row['TMP'] != "+9999,9":
                        tmp_parts = row['TMP'].split(',')
                        if len(tmp_parts) >= 1:
                            temperature = float(tmp_parts[0])
                            temperature_quality = tmp_parts[1]
                            temperatures.append(temperature)
                            temperatures_qualities.append(temperature_quality)
                            if nearest_station_id == station_id:
                                nearest_station_data["temp"] = temperature
                                nearest_station_data["temp_quality"] = temperature_quality

                    if has_aa1 and row['AA1']:
                        aa1_parts = row['AA1'].split(',')
                        if len(aa1_parts) >= 3 and aa1_parts[0] == "01":
                            precipitation = float(aa1_parts[1])
                            precipitations.append(precipitation)
                            if nearest_station_id == station_id:
                                nearest_station_data["precip"] = precipitation

    wind_avg = round(sum(winds) / len(winds), 2) if winds else None
    temp_avg = round(sum(temperatures) / len(temperatures), 2) if temperatures else None
    precip_avg = round(sum(precipitations) / len(precipitations), 2) if precipitations else None
    
    avg_distance = round(sum(distances) / len(distances), 2) if distances else None

    return {
        "Average Wind": wind_avg,
        "Average Temperature": temp_avg,
        "Average Precipitation": precip_avg,
        "Nearest Station Wind": nearest_station_data.get("wind"),
        "Nearest Station Temperature": nearest_station_data.get("temp"),
        "Nearest Station Temperature Quality": nearest_station_data.get("temp_quality"),
        "Nearest Station Precipitation": nearest_station_data.get("precip"),
        "Average Distance from Centroid": avg_distance,
        "Nearest Station Distance from Centroid": nearest_station_distance,
        "Nearest Station ID": nearest_station_id
    }

def main():
    file_path = 'formatted_data.json'
    csv_file_path = 'County Centroids.csv'

    with open(file_path, 'r') as file:
        json_data = json.load(file)
    
    fips_level_keys = get_keys(json_data)
    print(len(fips_level_keys))
    # time.sleep(2)
    
    # Initialize the output CSV file using the first FIPS code
    first_fips = list(fips_level_keys)[0]
    output_csv_filename = f"{first_fips}_data.csv"
    with open(output_csv_filename, mode='w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            "state", "county_fips", "average distance to stations (miles)", "station identifiers (USAF WBAN)", 
            "nearest station", "nearest station distance", "county_name", "average wind", "nearest wind", 
            "average temperature", "nearest temperature", "nearest temperature quality", "average precipitation", 
            "nearest precipitation", "hour", "day", "month", "year", "date"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',')
        writer.writeheader()

        print("FIPS level keys in the JSON file:")
        for fips in fips_level_keys:
            print(f"Processing FIPS: {fips}")
            print(json_data[f"{fips}"])
            county_name, state, county_lat, county_lon = get_county_info(csv_file_path, str(fips))
            print(f"County Name: {county_name}, State: {state}, Latitude: {county_lat}, Longitude: {county_lon}")
            county_centroid =  (float(county_lat), float(county_lon))
            if county_name is None:
                print(f"No data found for FIPS: {fips}")
                continue
            
            year_level_keys = get_keys(json_data[f"{fips}"])
            # time.sleep(2)
            for year in year_level_keys:
                print(json_data[f"{fips}"][year])
                print(year)
                # time.sleep(2)
                downloaded_files = []
                for nearby_station in json_data[f"{fips}"][year]["nearby_stations"]:
                    print(nearby_station)
                    downloaded_file = download_csv(nearby_station)
                    if downloaded_file:
                        downloaded_files.append(downloaded_file)
                    # time.sleep(2)

                for month_day in json_data[f"{fips}"][year]["month-day"]:
                    for hour in range(24):
                        print(downloaded_files, year, int(month_day[0]), int(month_day[1]))
                        avg_data = get_avg_and_nearest_station_data(downloaded_files, year, int(month_day[0]), int(month_day[1]), hour, county_centroid)

                        date_str = f"{year}-{int(month_day[0]):02d}-{int(month_day[1]):02d}T{hour:02d}"

                        nearest_temperature = avg_data.get('Nearest Station Temperature')
                        nearest_temperature_quality = avg_data.get('Nearest Station Temperature Quality')

                        row = {
                            "state": state,
                            "county_fips": fips,
                            "average distance to stations (miles)": avg_data.get("Average Distance from Centroid"),
                            "station identifiers (USAF WBAN)": "|".join([file.split('.')[0] for file in downloaded_files]),
                            "nearest station": avg_data.get("Nearest Station ID"),
                            "nearest station distance": round(avg_data.get("Nearest Station Distance from Centroid"), 2),
                            "county_name": county_name,
                            "average wind": avg_data.get("Average Wind"),
                            "nearest wind": avg_data.get("Nearest Station Wind"),
                            "average temperature": avg_data.get("Average Temperature"),
                            "nearest temperature": nearest_temperature,
                            "nearest temperature quality": nearest_temperature_quality,
                            "average precipitation": avg_data.get("Average Precipitation"),
                            "nearest precipitation": avg_data.get("Nearest Station Precipitation"),
                            "hour": hour,
                            "day": int(month_day[1]),
                            "month": int(month_day[0]),
                            "year": year,
                            "date": date_str
                        }
                        
                        writer.writerow(row)

                        print(f"For {date_str}:")
                        for key, value in avg_data.items():
                            print(f"{key}: {value}")
                        # time.sleep(2)

                # time.sleep(10)
                remove_files(downloaded_files)

if __name__ == "__main__":
    main()
