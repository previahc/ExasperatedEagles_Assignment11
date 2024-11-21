# Name: Quynh Doan, Denise Huynh, Andrew Mehlman, Heitor Previatti
# Email: doanqb@mail.uc.edu, previahc@mail.uc.edu, previahc@mail.uc.edu, mehlmadm@mail.uc.edu
# Assignment Number: Assignment 11
# Due Date: 11/21/2024
# Course #/Section: IS 4010-001
# Semester/Year: Fall Semester 2024
# Brief Description of the assignment: Clean and analyze fuel purchase data.
# Brief Description of what this module does: This module defines a DataCleaner class, which provides methods to load, clean, and process fuel purchase data from a CSV file. It performs various data cleaning operations, including removing duplicates, handling anomalies, standardizing fields, filling missing values using an external API, and fixing formatting inconsistencies. The module ensures data quality and prepares the dataset for analysis, saving the cleaned data to a new file for further use.
# Citations: chatgpt.com, copilot.com
# Anything else that's relevant: N/A

# dataCleaner.py
import pandas as pd
import os
import requests

class DataCleaner:
    """
    A class to clean and process fuel purchase data.
    """
    def __init__(self, file_path):
        self.file_path = file_path  # Input CSV file path
        self.data = None  # Placeholder for loaded data
        self.anomalies_file = os.path.join('Data', 'dataAnomalies.csv')
        self.cleaned_file = os.path.join('Data', 'cleanedData.csv')
        self.api_key = 'b1347430-a556-11ef-8cae-b9c4285d10f4'

        # Ensure Data folder exists
        if not os.path.exists('Data'):
            os.makedirs('Data')

    def load_data(self):
        """
        Loads data from the CSV file.
        """
        self.data = pd.read_csv(self.file_path, low_memory=False)
        print("Data loaded successfully.")

    def clean_gross_price(self):
            """Ensure 'Gross Price' values are rounded to 2 decimal places."""
            self.data['Gross Price'] = pd.to_numeric(self.data['Gross Price'], errors='coerce').round(2)
            print("Gross Price column cleaned.")
    def remove_duplicates(self):
        """Removes duplicate rows."""
        initial_count = len(self.data)
        self.data = self.data.drop_duplicates()
        final_count = len(self.data)
        print(f"Removed {initial_count - final_count} duplicate rows.")

    def handle_anomalies(self):
        """Separates 'Pepsi' purchases as anomalies."""
        pepsi_data = self.data[self.data['Fuel Type'] == 'Pepsi']
        fuel_data = self.data[self.data['Fuel Type'] != 'Pepsi']

        # Save anomalies
        pepsi_data.to_csv(self.anomalies_file, index=False)
        print(f"Anomalies saved to {self.anomalies_file}.")

        # Update main data
        self.data = fuel_data

    def save_clean_data(self):
        """Saves cleaned data to a CSV file."""
        self.data.to_csv(self.cleaned_file, index=False)
        print(f"Cleaned data saved to {self.cleaned_file}.")

    def fill_missing_zip_codes(self):
        """ 
        Fills missing zip codes in the 'Full Address' column using the Zipcodebase API.

        - Identifies rows with missing zip codes in the address.
        - Queries the API to fetch valid zip codes for city-state combinations.
        - Updates the address with the fetched zip codes.
        """
        self.data.reset_index(drop=True,inplace=True)
    # Dictionary to store city-state to zip code mapping
        city_state_zip_map = {}
        rows_to_update = []

        # Step 1: Identify rows that need zip codes and prepare city-state pairs
        for index, row in self.data.iterrows():
            full_address = row.get("Full Address", "")
            #print(full_address)
            if isinstance(full_address, str):
                # Check if zip code is missing
                if not any(part.isdigit() and len(part) == 5 for part in full_address.split(" ")):
                    parts = full_address.split(",")
                    if len(parts) >= 2:
                        city = parts[1].strip().split(" ")[0]
                        state = parts[-1].strip().split(" ")[0] if len(parts) > 2 else "Unknown"
                        city_state = (city, state)
                        rows_to_update.append((index, full_address, city_state))
                        # Store city-state in map if not already queried
                        if city_state not in city_state_zip_map:
                            city_state_zip_map[city_state] = None

        # Step 2: Query API for each unique city-state
        for city_state in city_state_zip_map.keys():
            city, state = city_state
            if state == "OH":
                state = "Ohio"
            params = {
                "city": city,
                "state_name": state,
                "country": "US"
            }
            try:
                response = requests.get(
                    'https://app.zipcodebase.com/api/v1/code/city',
                    params=params,
                    headers={"apikey": self.api_key}
                )
                if response.status_code == 200:
                    results = response.json().get('results', [])
                    if results:
                        city_state_zip_map[city_state] = results[0]  # Store the first zip code
            except Exception as e:
                print(f"Error fetching zip code for city-state {city_state}: {e}")

        # Step 3: Update rows with fetched zip codes
        for index, full_address, city_state in rows_to_update:
            zip_code = city_state_zip_map.get(city_state)
            if zip_code:
                self.data.at[index, "Full Address"] = f"{full_address} {zip_code}"
                #print(f"Added zip code {zip_code} to address: {full_address}.")


