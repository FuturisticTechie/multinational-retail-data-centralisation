

import psycopg2
import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd

class DatabaseConnector:
    def __init__(self, yaml_file):
        self.yaml_file = yaml_file

    def read_db_creds(self, yaml_file_path):
         with open(yaml_file_path, 'r') as file:
            db_creds = yaml.safe_load(file)
            return db_creds

    def init_db_engine(self):
        self.creds = self.read_db_creds(self.yaml_file)
        engine = create_engine(f"postgresql+psycopg2://{self.creds['RDS_USER']}:{self.creds['RDS_PASSWORD']}@{self.creds['RDS_HOST']}:{self.creds['RDS_PORT']}/{self.creds['RDS_DATABASE']}")
        return engine

    def list_db_tables(self, engine):
        inspector = inspect(engine)
        return inspector.get_table_names()

    def read_rds_table(self, con, table):
        df = pd.read_sql_table(table, con)
        return df

    def load_table(self):
        engine = self.init_db_engine()
        tables_list = self.list_db_tables(engine)
        print(tables_list)
        with engine.begin() as conn:
            self.table_data = pd.read_sql_table(tables_list[1], con=conn)
        return self.table_data   


if __name__ == '__main__':
    def main():
        # dc = DatabaseConnector('db_creds.yaml')  # Create an instance of DatabaseConnector
        # table = dc.load_table()  # Call the instance method on the created instance

        # table.to_csv('og_data.csv', index=False)
        # null_counts = table.isnull().sum()
        # print("Number of null values in each column:")
        # print(null_counts)


        dc = DatabaseConnector('db_creds.yaml')  # Create an instance of DatabaseConnector
        engine = dc.init_db_engine()  # Initialize the database engine

        # List all tables in the database
        tables_list = dc.list_db_tables(engine)
        print("List of tables in the database:")
        print(tables_list)

        orders_table_name = tables_list[2]  #'orders_table' is the third table
        print(f"Using orders table: {orders_table_name}")

        # Extract orders data from the 'orders_table'
        orders_data = dc.read_rds_table(engine, orders_table_name)
        # print("Orders DataFrame:")
        # print(orders_data)
        orders_data.to_csv('orders_data.csv', index=False)

    main()