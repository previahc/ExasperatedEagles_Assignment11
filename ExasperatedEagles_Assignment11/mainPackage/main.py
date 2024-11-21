# Name: Quynh Doan, Denise Huynh, Andrew Mehlman, Heitor Previatti
# Email: doanqb@mail.uc.edu, previahc@mail.uc.edu, previahc@mail.uc.edu, mehlmadm@mail.uc.edu
# Assignment Number: Assignment 11
# Due Date: 11/21/2024
# Course #/Section: IS 4010-001
# Semester/Year: Fall Semester 2024
# Brief Description of the assignment: Clean and analyze fuel purchase data.
# Brief Description of what this module does: The main.py module serves as the primary script to manage the data cleaning process for fuel purchase data. It utilizes the DataCleaner class to sequentially load the raw dataset, apply cleaning operations, and save the cleaned data to a new file. Each step addresses specific issues such as removing duplicates, handling anomalies, and standardizing fields like transaction numbers, fuel types, and site IDs. This module ensures the dataset is consistent, accurate, and ready for further analysis.
# Citations: chatgpt.com, copilot.com
# Anything else that's relevant: N/A

#main.py
from dataCleanerPackage.dataCleaner import DataCleaner


if __name__ == "__main__":
    # Initialize the DataCleaner class with the correct relative file path
    cleaner = DataCleaner('fuelPurchaseData/fuelPurchaseData.csv')
    
    print("Loading data...")
    cleaner.load_data()
    
    print("Cleaning gross prices...")
    cleaner.clean_gross_price()
    
    print("Removing duplicate rows...")
    cleaner.remove_duplicates()
    
    print("Handling anomalies (separating 'Pepsi' purchases)...")
    cleaner.handle_anomalies()
    
    print("Filling missing zip codes...")
    cleaner.fill_missing_zip_codes()

    print("Clean transaction number...")
    cleaner.clean_transaction_number()

    print("Clean fuel quantity...")
    cleaner.clean_fuel_quantity()

    print("Fix full address...")
    cleaner.fix_full_address()

    print("Standardize fuel type...")
    cleaner.standardize_fuel_type()

    print("Fix site id...")
    cleaner.fix_site_id()
    
    print("Saving cleaned data...")
    cleaner.save_clean_data()
    
    print("Data cleaning process completed.")
