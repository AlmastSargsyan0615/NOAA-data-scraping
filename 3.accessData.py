import csv

def get_formatted_date(year, month, day):
    return f"{month}-{day}"

def process_csv_file(file_path):
    data_dict = {}
    try:
        # Open the CSV file
        with open(file_path, mode='r') as file:
            # Create a CSV reader object
            reader = csv.DictReader(file)

            # Iterate over each row in the CSV file
            for row in reader:
                fips = row["county_fips"]
                year = row["year"]
                month = row["month"]
                day = row["day"]

                # Create or update nested dictionaries
                if fips not in data_dict:
                    data_dict[fips] = {year: [(month, day)]}
                elif year not in data_dict[fips]:
                    data_dict[fips][year] = [(month, day)]
                else:
                    data_dict[fips][year].append((month, day))

    except FileNotFoundError:
        print("The file 'necessaryFIPS.csv' does not exist.")

    return data_dict

# Example usage
file_path = 'necessaryFIPS.csv'
formatted_dates = process_csv_file(file_path)

# Print the formatted data
for fips, years in formatted_dates.items():
    print(f"FIPS: {fips}")
    for year, dates in years.items():
        print(f"\tYear: {year}")
        for month, day in dates:
            print(f"\t\t{month}-{day}")
