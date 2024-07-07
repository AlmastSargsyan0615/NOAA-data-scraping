import json
import time
import csv

def get_formatted_date(year, month, day):
    return f"{month}-{day}"

def process_csv_file(fips_file_path, station_file_path):
    data_dict = {}
    try:
        # Open the CSV file containing FIPS data
        with open(fips_file_path, mode='r') as fips_file:
            # Create a CSV reader object
            fips_reader = csv.DictReader(fips_file)

            # Iterate over each row in the FIPS CSV file
            for fips_row in fips_reader:
                fips_code = fips_row["county_fips"]
                year = fips_row["year"]
                month = fips_row["month"]
                day = fips_row["day"]

                # Open the CSV file containing station data
                with open(station_file_path, mode='r') as station_file:
                    # Create a CSV reader object
                    station_reader = csv.DictReader(station_file)

                    # Iterate over each row in the station CSV file
                    for station_row in station_reader:
                        if station_row["County_FIPS"] == fips_code:
                            nearby_station_ids = station_row["Station_Identifiers_(USAF_WBAN)"].split("|")
                            nearest_station_id = station_row["Nearest_Station"]

                            # Generate URLs for downloading CSV files for nearby stations
                            urls = [f"https://www.ncei.noaa.gov/data/global-hourly/access/{year}/{station_id}.csv" for station_id in nearby_station_ids]

                            # Generate URL for downloading CSV file for nearest station
                            nearest_station_url = f"https://www.ncei.noaa.gov/data/global-hourly/access/{year}/{nearest_station_id}.csv"

                            # Add data to the dictionary
                            if fips_code not in data_dict:
                                data_dict[fips_code] = {year: {"nearby_stations": urls, "nearest_station": nearest_station_url, "month-day": [(month, day)]}}
                            else:
                                if year not in data_dict[fips_code]:
                                    data_dict[fips_code][year] = {"nearby_stations": urls, "nearest_station": nearest_station_url, "month-day": [(month, day)]}
                                else:
                                    # Append month-day data to existing year data
                                    data_dict[fips_code][year]["month-day"].append((month, day))

                # Print data for verification
                print(fips_code)
                print(data_dict[fips_code])
                # time.sleep(100)

    except FileNotFoundError:
        print("One of the files does not exist.")

    return data_dict

# Function to save dictionary to a JSON file
def save_dict_to_json(dictionary, file_path):
    with open(file_path, 'w') as json_file:
        json.dump(dictionary, json_file)

# Example usage
fips_file_path = 'necessaryFIPS.csv'
station_file_path = 'county_station_statistics.csv'
formatted_data = process_csv_file(fips_file_path, station_file_path)

# Save the formatted data to a JSON file
save_dict_to_json(formatted_data, 'formatted_data.json')
