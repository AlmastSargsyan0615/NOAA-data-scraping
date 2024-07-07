import json
import time
def get_fips_level_keys(json_data):
    if isinstance(json_data, dict):
        return json_data.keys()
    return []
import os
import requests

def download_csv(url):
    """
    Download a CSV file from the given URL and save it with the filename extracted from the URL.
    
    Args:
        url (str): The URL of the CSV file.
        
    Returns:
        bool: True if the file was downloaded successfully, False otherwise.
    """
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Extract filename from the URL
            filename = os.path.basename(url)
            
            # Save the content of the response (CSV file) to a local file
            with open(filename, "wb") as f:
                f.write(response.content)
            print("File downloaded successfully.")
            return True
        else:
            print("Failed to download the file. Status code:", response.status_code)
            return False
    except Exception as e:
        print("An error occurred:", e)
        return False


def main():
    file_path = 'formatted_data.json'  # Replace 'your_json_file_path.json' with your actual file path
    with open(file_path, 'r') as file:
        json_data = json.load(file)
    
    fips_level_keys = get_fips_level_keys(json_data)
    print(len(fips_level_keys))
    time.sleep(2)
    
    print("FIPS level keys in the JSON file:")
    for key in fips_level_keys:
        print(json_data[f"{key}"])
        print(key)
        fips_level_keyssss = get_fips_level_keys(json_data[f"{key}"])
        time.sleep(2)
        for key1 in fips_level_keyssss:
            print(json_data[f"{key}"][key1])
            print(key1)
            time.sleep(2)
            for value1 in json_data[f"{key}"][key1]["nearby_stations"]:
                print(value1)
                download_csv(value1)
                time.sleep(2)
            print("rrrrrrrrrrrrrrrrrr", json_data[f"{key}"][key1]["nearest_station"])
            time.sleep(2)
        time.sleep(10)
if __name__ == "__main__":
    main()
