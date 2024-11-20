    

def clean_gross_price(self):
        """Ensure 'Gross Price' values are rounded to 2 decimal places."""
        self.data['Gross Price'] = pd.to_numeric(self.data['Gross Price'], errors='coerce').round(2)
        print("Gross Price column cleaned.")
