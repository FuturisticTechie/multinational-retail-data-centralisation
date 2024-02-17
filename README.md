

#Multinational Retail Data Centralisation

##Project description
This project entails gathering data for a multinational company that sells various goods across the globe. This involves consolidating their sales data is spread across many different data sources (pdf, api and s3) making it not easily accessible or analysable by current members of the team. This project aims to make its sales data accessible from one centralised location, helping achieve the orgnaisations efforts to become more data-driven, and easily be able to query the database to get up-to-date metrics for the business.

##Project Milestones
1. Setting up GitGub where the code for the projct is stored, updated and backed up.
2. Extracting and cleaning the data from the multiple data sources, and then storing it the database.
3. Create and develop a database schema of the database, esnuring that the columns are of the correct data types
4. Querying the data to obtain up-to-date metrics, to facilitate making data-driven decisions and get a better understanding of sales

##File structure 
#database_utils.py
Connects and uploads to database on pgAdmin4 using 'DatabaseConnector' class

#data_extraction.py
Data extraction using 'DataExtractor' class, from pdf, apt and s3 sources

#data_cleaning.py
Where the data cleaning code is written based on 'DataCleaning' class

#main.py
Where the data_cleaning.py and uploading process is executed

#sql_schema
Contains code that further clarifed columsna dn creates primary and foriegn keys 

#sql_queries
Contains sql quesries relaying to specific burinsess questions 

##Data sources

1. AWS RDS Database
- Data source: Historical sales and user data stored in an AWS RDS database.
- Extraction Method: Utilized methods in the data_extraction and database_utils classes.
- Tables Extracted: orders_table, dim_users
2. AWS S3 Bucket
- Data source: Products data saved as a CSV file in an AWS S3 bucket.
- Extraction Method: Leveraged boto3 for downloading and extraction. This is then turned into a Pandas DataFrame.
- Tables Extracted: dim_products, dim_date_times
3. AWS Link (PDF)
- Data source: PDF file stored in an AWS S3 bucket.
- Extraction Method: Utilized tabula to read tables from the PDF and convert them into a pandas DataFrame.
- Tables Extracted: dim_card_details
4. RESTful API
- Data source: Store data retrieved from an API endpoint.
- Extraction Method: HTTP GET requests to the API, followed by normalization of the originally received JSON data. 
- Tables Extracted: dim_store_details


#This code is free to use 

#Data Science and Data Engineering Tools
Python (Pandas, NumPy)
PostgreSQL
AWS (boto3)
SQLalchemy
psycopg2
tabula-py
YAML (library)