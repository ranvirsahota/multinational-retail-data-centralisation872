from database_utils import DatabaseConnector
import sqlalchemy as sqlalc
import pandas as pd
import tabula
import requests
import numpy as np
import boto3
import csv

class DataExtractor:
    '''
    Parameters:
    ----------

    Attributes:
    ----------

    Methods:
    -------
    list_db_tables()
        Lists all tables in database, so you know which ones are avaliable
    read_rds_table()
        To extract database table to a pandas DataFrame
    retrieve_pdf_data()

    list_number_of_stores()

    retrieve_stores_data()
    
    '''

    def _get_response(self, url:str):
        response = requests.get(url)
        if response.status_code == 200:
            print(f'Success: {response}')
            return response
        else:
            response.raise_for_status()
    
    def list_db_tables(self, db_connect):
        return pd.DataFrame(db_connect
                            .execute_query('SELECT * FROM pg_catalog.pg_tables;'))

    def read_rds_table(self, query, db_connect):
        result = db_connect.execute_query(query)
        return pd.DataFrame(result)

    def retrieve_pdf_data(self, pdf_url:str):
        cards_dataframe_list = tabula.read_pdf(input_path=pdf_url, pages='all')
        cards_dataframe_merged = pd.DataFrame()
        for df in cards_dataframe_list:
            cards_dataframe_merged = pd.concat([cards_dataframe_merged, df])
        return cards_dataframe_merged
    
    def list_number_of_stores(self, endpoint_url:str, header:dict):
        response = requests.get(url = endpoint_url, headers = header).json()
        return response['number_stores']

    def retrieve_stores_data(self, endpoint_url:str, header:dict):
        total_stores = self.list_number_of_stores(endpoint_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',
                                                header = header)
        final_stores_df = pd.DataFrame()
        for store_num in range(total_stores):
            response = requests.get(url = endpoint_url + f'{store_num}', headers = header).json()
            store_df = pd.DataFrame({
                'index': [response['index']],
                'address': [response['address']],
                'longitude': [response['longitude']],
                'lat': [response['lat']],
                'locality': [response['locality']],
                'store_code': [response['store_code']],
                'staff_numbers': [response['staff_numbers']],
                'opening_date': [response['opening_date']],
                'store_type': [response['store_type']],
                'latitude': [response['latitude']],
                'country_code': [response['country_code']],
                'continent': [response['continent']]
            })
            final_stores_df = pd.concat([final_stores_df, store_df], ignore_index=True)
        return final_stores_df
    
    def extract_from_s3(self, s3_url:str):
        path_parts = s3_url.replace("s3://","").split("/")
        bucket = path_parts.pop(0)
        key = "/".join(path_parts)
        s3 = boto3.client('s3')
        s3.download_file(bucket, key, 's3_products.csv')
        with open('s3_products.csv', newline='\n', mode='r') as file:
            csv_reader = csv.reader(file, delimiter = ',')
            column_names = next(csv_reader)
            column_names[0] = 'index'
            return pd.DataFrame(columns=column_names, data=csv_reader)
    
    def get_sales_data(self, url):
        response = self._get_response(url)
        return pd.DataFrame(response.json())
    