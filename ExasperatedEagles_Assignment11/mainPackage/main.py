# Name: Quynh Doan, Denise Huynh, Andrew Mehlman, Heitor Previatti
# Email: doanqb@mail.uc.edu, previahc@mail.uc.edu, previahc@mail.uc.edu, mehlmadm@mail.uc.edu
# Assignment Number: Assignment 11
# Due Date: 11/21/2024
# Course #/Section: IS 4010-001
# Semester/Year: Fall Semester 2024
# Brief Description of the assignment: Clean and analyze fuel purchase data.
# Brief Description of what this module does: Provides methods to load, clean, and save processed data.
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
    
    print("Saving cleaned data...")
    cleaner.save_clean_data()
    
    print("Data cleaning process completed.")
