
import pandas as pd
import re
from datetime import datetime
from database_utils import DatabaseConnector
import numpy as np
from sqlalchemy import create_engine 

class DataCleaning:
    pd.options.mode.chained_assignment = None
    def __init__(self):
        self.table = None

    def load_data(self):
        dc = DatabaseConnector('db_creds.yaml')
        dc.load_table()  # Load the table and store it in the attribute
        self.table = dc.table_data.copy()  # Store the table as an instance variable

    def clean_user_data(self):
        self.table = self.handle_null_values()
        self.table = self.handle_date_errors(date_column='date_of_birth')
        self.table = self.handle_date_errors(date_column='join_date')
        self.table = self.handle_country_error(country_column='country')
        self.table = self.handle_country_code_error(country_code='country_code')
        self.table = self.clean_uuid_column(uuid_column='user_uuid')
        self.table = self.remove_column(column_name='index')


    def handle_null_values(self):
        self.table.replace('NULL', pd.NA, inplace=True)      #Replace 'NULL' values with NaN
        self.table.dropna(how='all')          #Drops rows if all NaN
        
        return self.table

    def handle_date_errors(self, date_column):
        self.table.loc[:, date_column] = pd.to_datetime(self.table[date_column], format='mixed', errors='coerce')  # sets invalid dates to NaT
        self.table.loc[pd.isna(self.table[date_column]), date_column] = None                        # Set invalid dates to None
        return self.table
  

    def handle_country_error(self, country_column):
        country_list = ['United Kingdom', 'Germany', 'United States']
        mask = self.table[country_column].isin(country_list)
        self.table.loc[~mask, country_column] = np.nan
        self.table.dropna(subset=[country_column], inplace=True)
        return self.table

    def handle_country_code_error(self, country_code):
        # Replace 'GGB' with 'GB'
        self.table[country_code] = self.table[country_code].replace('GGB', 'GB')

        code_list = ['DE', 'GB', 'US']
        mask = self.table[country_code].isin(code_list)
        self.table.loc[~mask, country_code] = np.nan
        self.table.dropna(subset=[country_code], inplace=True)
        return self.table

    def clean_uuid_column(self, uuid_column):
        pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        for i, uuid_value in enumerate(self.table[uuid_column]):
            if not re.match(pattern, str(uuid_value)):
                self.table.loc[i, uuid_column] = np.nan  # Replace 'NULL' values with NaN
        self.table.dropna(subset=[uuid_column], inplace=True)  # Drops rows with any NaN values
        return self.table

    def handle_longitude(self, longitude):
        pattern = r'^\d+\.?\d*$'
        for i, value in enumerate(self.table[longitude]):
            if not re.match(pattern, str(value)):
                self.table.loc[i, longitude] = np.nan  # Replace invalid values with NaN
        self.table[1:].dropna(subset=[longitude], inplace=True) #spares 1st row from being dropped
        return self.table
    
    def handle_latitude(self, latitude):
        pattern = r'^[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?)$'
        for i, value in enumerate(self.table[latitude]):
            if not re.match(pattern, str(value)):
                self.table.loc[i, latitude] = np.nan  # Replace invalid values with NaN
        self.table[1:].dropna(subset=[latitude], inplace=True)   #spares 1st row from being dropped
        return self.table


    def upload_to_db(self, dataframe, table_name, include_index=True):      #uploads to database
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = '123456'
        DATABASE = 'sales_data'
        PORT = 5432
        engine_2 = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        dataframe.to_sql(table_name,engine_2,if_exists = 'replace',index=include_index)

    def import_processed_data(self, file_path):
        self.table = pd.read_csv(file_path)
    
    def clean_card_data(self):
        self.table = self.handle_null_values()
        self.table = self.handle_date_errors(date_column='date_payment_confirmed')
        self.table = self.clean_card_number(card_column='card_number')
        return self.table
    
    def clean_card_number(self, card_column):
        # Apply the string cleaning operation
        self.table[card_column] = self.table[card_column].str.replace(r'\D', '', regex=True)  
        # Filter out rows with card numbers less than 5 characters long
        self.table = self.table[self.table[card_column].str.len() >= 5]
        
        # Return the modified DataFrame
        return self.table



    def remove_column(self, column_name):
        self.table.drop(columns=[column_name], inplace=True)
        return self.table


    def clean_store_data(self):
        self.table = self.handle_null_values()
        self.table = self.handle_date_errors(date_column='opening_date')
        self.table = self.remove_column(column_name='lat')
        self.table = self.handle_longitude(longitude='longitude')
        self.table = self.handle_latitude(latitude='latitude')
        self.table = self.remove_invalid_staff_numbers(staff_column='staff_numbers')
        self.table = self.handle_country_code_error(country_code='country_code')
        self.table = self.remove_column(column_name='index')
        return self.table


    def remove_invalid_staff_numbers(self, staff_column):
        # Preprocess staff numbers to remove letters
        self.table[staff_column] = self.table[staff_column].str.extract(r'(\d+)')
            
        # Use pandas to_numeric to convert the column to numeric values,
        # and keep only rows with valid numeric values
        self.table[staff_column] = pd.to_numeric(self.table[staff_column], errors='coerce')
        self.table.dropna(subset=[staff_column], inplace=True)

        return self.table

    def convert_product_weights(self):
        self.table['weight'] = self.table['weight'].apply(self.convert_weight_to_kg)
        return self.table

    def convert_weight_to_kg(self, weight):
        if isinstance(weight, (int, float)):
            return weight  # If weight is already a number, return it as is
        
        if 'g' in weight:
            return float(re.sub(r'\D', '', weight)) / 1000  # Convert grams to kilograms
        elif 'ml' in weight:
            return float(re.sub(r'\D', '', weight)) / 1000  # Convert milliliters to kilograms
        else:
            try:
                return float(weight)
            except ValueError:
                return None 
            
    def clean_products_data(self):
        self.table = self.handle_null_values()
        self.table = self.handle_date_errors(date_column='date_added')
        self.table = self.clean_uuid_column(uuid_column='uuid')
        self.table = self.remove_column(column_name='Unnamed: 0')
        return self.table


    def clean_orders_data(self):
        self.table = self.handle_null_values()
        self.table = self.remove_column(column_name='first_name')
        self.table = self.remove_column(column_name='last_name')
        self.table = self.remove_column(column_name='1')
        self.table = self.remove_column(column_name='level_0')
        self.table = self.clean_uuid_column(uuid_column='date_uuid')
        self.table = self.clean_uuid_column(uuid_column='user_uuid')
        self.table = self.remove_column(column_name='index')
        return self.table
    
    def clean_date_details(self):
        self.table = self.handle_null_values()
        self.table = self.clean_uuid_column(uuid_column='date_uuid')
        return self.table


data_cleaner = DataCleaning()
#code in main.py