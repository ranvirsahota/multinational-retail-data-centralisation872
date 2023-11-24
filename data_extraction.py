from database_utils import DatabaseConnector
import sqlalchemy as sqlalc
import pandas as pd
import tabula
import requests
import numpy as np
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

    
    '''

    def list_db_tables(self, db_conn:DatabaseConnector):
        with db_conn.init_db_engine().connect() as engine_conn:
            result = engine_conn.execute(sqlalc.text('SELECT * FROM pg_catalog.pg_tables;'))

    def read_rds_table(self, db_conn:DatabaseConnector, tbl_name:str):
        with db_conn.init_db_engine().connect() as engine_conn:
            result = engine_conn.execute(sqlalc.text(f'SELECT * FROM {tbl_name};'))
            return pd.DataFrame(result)
        
    def retrieve_pdf_data(self, pdf_url:str):
        dataframes_list = tabula.read_pdf(input_path=pdf_url, pages='all')
        df_merged = pd.DataFrame()
        
        for df in dataframes_list:
            df_merged = pd.concat([df_merged, df])
        
        return df_merged
    
    def list_number_of_stores(self, endpoint_url:str, header:dict):
        response = requests.get(url = endpoint_url, headers = header).json()
        return response['number_stores']
    

    def retrieve_stores_data(self, endpoint_url:str, header:dict):
        total_stores = self.list_number_of_stores(endpoint_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',
                                                header = header)
        df_final = pd.DataFrame()
        for store_num in range(total_stores):
            response = requests.get(url = endpoint_url + f'{store_num}', headers = header).json()
            df_dict = pd.DataFrame({
                'index': [response['index']],
                'address': [response['address'] if response['address'] != 'N/A' else np.nan],
                'longitude': [response['longitude'] if response['longitude'] != 'N/A' else np.nan],
                'lat': [response['lat'] if response['lat'] != 'N/A' else np.nan],
                'locality': [response['locality'] if response['locality'] != 'N/A' else np.nan],
                'store_code': [response['store_code']],
                'staff_numbers': [response['staff_numbers']],
                'opening_date': [response['opening_date']],
                'store_type': [response['store_type']],
                'latitude': [response['latitude']],
                'country_code': [response['country_code']],
                'continent': [response['continent']]
            })

            df_final = pd.concat([df_final, df_dict])

        print(df_final)

        return df_final


DataExtractor().retrieve_stores_data(endpoint_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/',
                                     header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})

