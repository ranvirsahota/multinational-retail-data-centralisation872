from database_utils import DatabaseConnector
import sqlalchemy as sqlalc
import pandas as pd
import tabula
import requests
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
    
    def list_number_of_stores():
        response = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}')