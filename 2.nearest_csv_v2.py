import pandas as pd
import numpy as np
from geopy.distance import geodesic

# Read the CSV file
county_centroids_df = pd.read_csv("County Centroids.csv")
station_informations_df = pd.read_csv("station_data_2024-05-14_18-54-47.csv")

# Open the CSV file to write the information
with open("county_station_statistics_v2.csv", "w") as csv_file:

    # Write headers to the CSV file
    csv_file.write("State,County_FIPS,Nearby_Station_Identifiers_(USAF_WBAN),County_Name\n")

    def get_stations_by_state(station_informations_df, state):
        """
        Extracts station information from a DataFrame containing weather station data,
        filtered by state.

        Args:
        station_informations_df (DataFrame): DataFrame containing weather station data.
        state (str): The state code for filtering station information.

        Returns:
        DataFrame: DataFrame containing station information filtered by the specified state.
        """
        if state == "":
            return station_informations_df

        # Filter the DataFrame for stations in the desired state or with an empty state
        stations_in_state = station_informations_df[(station_informations_df['state'] == state) | (station_informations_df['state'].fillna('') == "")]

        return stations_in_state

    def get_nearby_stations_count_avg_distance(row_county, station_informations, max_distance=50):
        """
        Calculate nearby stations count, average distance, nearest distance, and nearest station ID.

        Args:
        row_county (Series): Series containing information about the county centroid.
        station_informations (DataFrame): DataFrame containing weather station information.
        max_distance (float): Maximum distance to consider a station as nearby (default: 50 miles).

        Returns:
        int: Count of nearby stations.
        float: Average distance to nearby stations.
        float: Nearest distance to a station.
        str: Station ID of the nearest station.
        """

        nearby_stations = []
        distances = []
        nearby_stations_str = ""
        for index, row in station_informations.iterrows():
            latitude_station = row['latitude']
            longitude_station = row['longitude']
            if not pd.isnull(latitude_station) and not pd.isnull(longitude_station):
                distance = geodesic((row_county["Latitude"], row_county["Longitude"]), (latitude_station, longitude_station)).miles
                if distance < max_distance:
                    nearby_stations.append((row['usaf'], row['wban']))
                    distances.append(distance)
                    if nearby_stations_str == "":
                        nearby_stations_str = str(row['usaf']) + str(row['wban'])
                    else:
                        nearby_stations_str = nearby_stations_str + "|" + str(row['usaf']) + str(row['wban'])

        nearby_stations_count = len(nearby_stations)
        avg_distance = np.mean(distances) if distances else np.nan
        nearest_distance = min(distances) if distances else np.nan
        nearest_station = nearby_stations[distances.index(nearest_distance)] if nearby_stations else (np.nan, np.nan)

        return nearby_stations_count, avg_distance, nearest_distance, f"{nearest_station[0]}{nearest_station[1]}", nearby_stations_str


    # Iterate over each row in the county centroid DataFrame
    for index, row_county in county_centroids_df.iterrows():
        print(f"Processing County: {row_county['County_Name']} ({row_county['FIPS']}) in {row_county['State']}...")

        # Filter stations by state
        station_informations = get_stations_by_state(station_informations_df, row_county["State"])

        # Get nearby stations statistics
        nearby_stations_count, avg_distance, nearest_distance, nearest_station_id, nearby_stations_str = get_nearby_stations_count_avg_distance(row_county, station_informations)

        # Write data for the current county to the CSV file
        csv_file.write("{},{},{},{}\n".format(
            row_county['State'],
            row_county['FIPS'],
            nearby_stations_str,
            row_county['County_Name']
        ))

