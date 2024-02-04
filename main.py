


from data_cleaning import DataCleaning 

data_cleaner = DataCleaning()
# data_cleaner.load_data()
# data_cleaner.clean_user_data()
# data_cleaner.table.to_csv('cleaned_data.csv', index=False)
# # print(data_cleaner.table)
# data_cleaner.upload_to_db(data_cleaner.table, 'dim_users')

# # Check for missing values
# missing_values = data_cleaner.table.isnull().sum()
# # print(missing_values)


# # Check data types
# data_types = data_cleaner.table.dtypes
# print(data_types) 


#-----------------------------------------------------------------------


# data_cleaner.import_processed_data('card_data.csv')
# data_cleaner.clean_card_data()
# # data_cleaner.table.to_csv('cleaned_card_data.csv', index=False)
# # print(data_cleaner.table)

# data_cleaner.upload_to_db(data_cleaner.table, 'dim_card_details')


#-----------------------------------------------------------------------

# data_cleaner.import_processed_data('stores_data.csv')
# data_cleaner.clean_store_data()

# data_cleaner.table.dropna(subset=['address'], inplace=True)
# data_cleaner.table.dropna(subset=['opening_date'], inplace=True)

# Check for missing values
# missing_values = data_cleaner.table.isnull().sum()
# print(missing_values)

# rows_with_missing_address = data_cleaner.table[data_cleaner.table['opening_date'].isnull()]
# print(rows_with_missing_address)

# print(data_cleaner.table)
# data_cleaner.table.to_csv('cleaned_store_data.csv', index=False)

# data_cleaner.upload_to_db(data_cleaner.table, 'dim_store_details')


#-----------------------------------------------------------------------


# data_cleaner.import_processed_data('products_data.csv')
# data_cleaner.convert_product_weights()
# data_cleaner.clean_products_data()
# # print(data_cleaner.table.head())

# data_cleaner.table.at[1841, 'weight'] = 0.454

# # Check for missing values
# missing_values = data_cleaner.table.isnull().sum()
# print(missing_values)

# rows_with_missing_address = data_cleaner.table[data_cleaner.table['weight'].isnull()]
# print(rows_with_missing_address)

# data_cleaner.table.to_csv('cleaned_data_kg.csv', index=False)

# data_cleaner.upload_to_db(data_cleaner.table, 'dim_products')

#-----------------------------------------------------------------------

# data_cleaner.import_processed_data('orders_data.csv')
# data_cleaner.clean_orders_data()
# # # print(data_cleaner.table)
# # data_cleaner.table.to_csv('clean_order_data.csv', index=False)

# # Check for missing values
# missing_values = data_cleaner.table.isnull().sum()
# print(missing_values)

# data_cleaner.upload_to_db(data_cleaner.table, 'orders_table')

#-----------------------------------------------------------------------

# data_cleaner.import_processed_data('date_details.csv')
# data_cleaner.clean_date_details()
# print(data_cleaner.table)
# data_cleaner.table.to_csv('clean_date_times.csv', index=False)

# Check for missing values
# missing_values = data_cleaner.table.isnull().sum()
# print(missing_values)

# data_cleaner.upload_to_db(data_cleaner.table, 'dim_date_times')
