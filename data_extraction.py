
import requests
import pandas as pd
import tabula 
import boto3
from io import BytesIO 

class DataExtractor:
    def __init__(self):
        pass

    def retrieve_pdf_data(self, pdf_link, headers=None):
        pdf_table = tabula.read_pdf(pdf_link, pages='all')
        if pdf_table:
            if len(pdf_table) > 1:
                pdf_df = pd.concat(pdf_table, ignore_index=True)
            else:
                pdf_df = pdf_table[0]
        return pdf_df

    def list_number_of_stores(self, endpoints, headers=None):
        response = requests.get(endpoints['no_of_stores'], headers=headers)
        data = response.json()
        return data.get('number_stores', None)

    def retrieve_stores_data(self, store_endpoint, headers=None):
        # Initialize an empty list to hold the store data
        store_data = []

        # Loop over all possible store numbers
        for store_number in range(0, 451):
            # Construct the store endpoint URL using the current store number
            url = store_endpoint.format(store_number=store_number)

            # Make a request to the store endpoint
            response = requests.get(url, headers=headers)

            # Extract the store data from the response JSON
            store_json = response.json()

            # Append the store data to the store_data list
            store_data.append(store_json)

        # Convert the store data to a pandas DataFrame
        store_data_df = pd.DataFrame(store_data)

        # Return the DataFrame
        return store_data_df
    

    def extract_from_s3(self, s3_address, aws_access_key_id=None, aws_secret_access_key=None):
        # Parse S3 address
        s3_address_parts = s3_address.replace("s3://", "").split("/")
        bucket_name = s3_address_parts[0]
        file_path = "/".join(s3_address_parts[1:])

        # Create S3 client
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        # Download file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_path)
        content = response["Body"].read()

        # Read content into DataFrame
        df = pd.read_csv(BytesIO(content))

        return df

    def extract_json_from_s3(self, s3_address, aws_access_key_id=None, aws_secret_access_key=None):
        # Parse S3 address
        s3_address_parts = s3_address.replace("s3://", "").split("/")
        bucket_name = s3_address_parts[0]
        file_path = "/".join(s3_address_parts[1:])

        # Create S3 client
        s3 = boto3.client(
            "s3",
            aws_access_key_id='AKIAQ3EGWHFV6HOUWSFZ',
            aws_secret_access_key='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'
        )

        # Download file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_path)
        content = response["Body"].read()

        # Assuming the content is JSON, load it into a DataFrame
        data = pd.read_json(BytesIO(content))

        return data




data_extractor = DataExtractor()
pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
# pdf_df = data_extractor.retrieve_pdf_data(pdf_link)



# pdf_df.to_csv('card_data.csv', index=False)
# null_counts = pdf_df.isnull().sum()
# print("Number of null values in each column:")
# print(null_counts)
# print(pdf_df)

# Example usage
api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
endpoints = {
    'store_retrieval': 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}',
    'no_of_stores': 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
}

# Use the same instance for both operations
# Step 1: List number of stores
number_of_stores = data_extractor.list_number_of_stores(endpoints, headers={'x-api-key': api_key})
# print(f"Number of stores: {number_of_stores}")

# Step 2: Assuming you want to retrieve all stores
# Step 3: Retrieve stores data
stores_df = data_extractor.retrieve_stores_data(endpoints['store_retrieval'], headers={'x-api-key': api_key})
# stores_df.to_csv('stores_data.csv', index=False)
# print(stores_df)

# s3_address = "s3://data-handling-public/products.csv"
# # Provide AWS access key explicitly 
# aws_access_key_id = "REDACTED"
# aws_secret_access_key = "REDACTED"
# products_data = data_extractor.extract_from_s3(s3_address, aws_access_key_id, aws_secret_access_key)

# print(products_data.head())
# products_data.to_csv('products_data.csv', index=False)
 

s3_address = "s3://data-handling-public/date_details.json"
# Provide AWS access key explicitly 
aws_access_key_id = "REDACTED"
aws_secret_access_key = "REDCATED"
date_details = data_extractor.extract_json_from_s3(s3_address, aws_access_key_id, aws_secret_access_key)

# print(products_data.head())
# date_details.to_csv('date_details.csv', index=False)