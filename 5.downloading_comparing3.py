import os
import json
import time
import requests
import pandas as pd
import csv
from geopy.distance import geodesic
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count

def get_keys(json_data):
    if isinstance(json_data, dict):
        return json_data.keys()
    return []

def download_csv(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            filename = os.path.basename(url)
            with open(filename, "wb") as f:
                f.write(response.content)
            return filename
        else:
            return None
    except Exception as e:
        return None

def remove_files(file_paths):
    for file_path in file_paths:
        try:
            os.remove(file_path)
        except Exception as e:
            print(f"An error occurred while removing {file_path}: {e}")

def get_county_info(file_path, fips):
    data = pd.read_csv(file_path)
    data['FIPS'] = data['FIPS'].astype(str)
    filtered_data = data[data['FIPS'] == fips]
    if not filtered_data.empty:
        county_name = filtered_data['County_Name'].values[0]
        state = filtered_data['State'].values[0]
        latitude = filtered_data['Latitude'].values[0]
        longitude = filtered_data['Longitude'].values[0]
        return county_name, state, latitude, longitude
    else:
        return None, None, None, None

def get_nearest_station_data(file_paths, year, month, day, hour, county_centroid):
    year = int(year)
    month = int(month)
    day = int(day)
    hour = int(hour)
    
    date_str = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}"
    
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
                            if nearest_station_id == station_id:
                                nearest_station_data["wind"] = wind_speed

                    if has_tmp and row['TMP'] and row['TMP'] != "+9999,9":
                        tmp_parts = row['TMP'].split(',')
                        if len(tmp_parts) >= 1:
                            temperature = float(tmp_parts[0])
                            temperature_quality = tmp_parts[1]
                            if nearest_station_id == station_id:
                                nearest_station_data["temp"] = temperature
                                nearest_station_data["temp_quality"] = temperature_quality

                    if has_aa1 and row['AA1']:
                        aa1_parts = row['AA1'].split(',')
                        if len(aa1_parts) >= 3 and aa1_parts[0] == "01":
                            precipitation = float(aa1_parts[1])
                            if nearest_station_id == station_id:
                                nearest_station_data["precip"] = precipitation

    return {
        "Nearest Station Wind": nearest_station_data.get("wind"),
        "Nearest Station Temperature": nearest_station_data.get("temp"),
        "Nearest Station Temperature Quality": nearest_station_data.get("temp_quality"),
        "Nearest Station Precipitation": nearest_station_data.get("precip"),
        "Nearest Station Distance from Centroid": nearest_station_distance,
        "Nearest Station ID": nearest_station_id
    }

def get_keys_between_from_json(lst, filename):
    try:
        lst = list(lst)
        with open(filename, 'r') as file:
            data = json.load(file)
            start_key = data["start_key"]
            end_key = data["end_key"]
        
        start_index = lst.index(start_key)
        end_index = lst.index(end_key)
        return lst[start_index:end_index+1]
    except ValueError as e:
        return str(e)
    except (FileNotFoundError, KeyError, json.JSONDecodeError) as e:
        return str(e)

def process_fips(fips, json_data, csv_file_path, output_csv_filename):
    county_name, state, county_lat, county_lon = get_county_info(csv_file_path, str(fips))
    if county_name is None:
        return
    
    county_centroid = (float(county_lat), float(county_lon))
    
    year_level_keys = get_keys(json_data[f"{fips}"])
    
    for year in year_level_keys:
        downloaded_files = []
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_url = {executor.submit(download_csv, url): url for url in json_data[f"{fips}"][year]["nearby_stations"]}
            for future in as_completed(future_to_url):
                downloaded_file = future.result()
                if downloaded_file:
                    downloaded_files.append(downloaded_file)

        for month_day in json_data[f"{fips}"][year]["month-day"]:
            for hour in range(24):
                nearest_data = get_nearest_station_data(downloaded_files, year, int(month_day[0]), int(month_day[1]), hour, county_centroid)
                date_str = f"{year}-{int(month_day[0]):02d}-{int(month_day[1]):02d}T{hour:02d}"
                
                nearest_temperature = nearest_data.get('Nearest Station Temperature')
                nearest_temperature_quality = nearest_data.get('Nearest Station Temperature Quality')

                row = {
                    "state": state,
                    "county_fips": fips,
                    "station identifiers (USAF WBAN)": "|".join([file.split('.')[0] for file in downloaded_files]),
                    "nearest station": nearest_data.get("Nearest Station ID"),
                    "nearest station distance": round(nearest_data.get("Nearest Station Distance from Centroid"), 2),
                    "county_name": county_name,
                    "nearest wind": nearest_data.get("Nearest Station Wind"),
                    "nearest temperature": nearest_temperature,
                    "nearest temperature quality": nearest_temperature_quality,
                    "nearest precipitation": nearest_data.get("Nearest Station Precipitation"),
                    "hour": hour,
                    "day": int(month_day[1]),
                    "month": int(month_day[0]),
                    "year": year,
                    "date": date_str
                }

                with open(output_csv_filename, mode='a', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=row.keys(), delimiter=',')
                    writer.writerow(row)

        remove_files(downloaded_files)

def main():
    file_path = 'formatted_data.json'
    csv_file_path = 'County Centroids.csv'
    start_end_path = "start_end.json"

    with open(file_path, 'r') as file:
        json_data = json.load(file)
    
    fips_level_keys = get_keys(json_data)
    fips_level_keys = get_keys_between_from_json(fips_level_keys, start_end_path)
    
    first_fips = list(fips_level_keys)[0]
    output_csv_filename = f"{first_fips}_data.csv"
    
    with open(output_csv_filename, mode='w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            "state", "county_fips", "station identifiers (USAF WBAN)", 
            "nearest station", "nearest station distance", "county_name", "nearest wind", 
            "nearest temperature", "nearest temperature quality", "nearest precipitation", 
            "hour", "day", "month", "year", "date"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',')
        writer.writeheader()

    num_cores = cpu_count()
    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        futures = [executor.submit(process_fips, fips, json_data, csv_file_path, output_csv_filename) for fips in fips_level_keys]
        for future in as_completed(futures):
            future.result()

if __name__ == "__main__":
    main()
