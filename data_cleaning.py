from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd

class DataCleaning:
    '''
    Parameters:
    ----------


    Attributes:
    ----------


    Methods:
    -------
    
    '''

    def clean_user_data(self):
        data_extrc = DataExtractor()
        db_conn = DatabaseConnector()
        df = data_extrc.read_rds_table(db_conn, 'legacy_users')
        df.set_index('index', inplace=True)
        # Chanage 'GGB' to 'GB', all rows with 'GGB' are in the UK
        df['country_code'] = df['country_code'].replace('GGB', 'GB')

        # remove all rows with null values or errornous data, 
        # Can be find by find all country codes not two characters
        df_mask = df['country_code'].str.len() == 2
        df = df[df_mask]


        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='mixed', errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], format='mixed', errors='coerce')

        df.first_name = df.first_name.astype('string')
        df.last_name = df.last_name.astype('string')
        df.date_of_birth = df.date_of_birth.astype('datetime64[ns]')
        df.company = df.company.astype('string')
        df.email_address = df.email_address.astype('string')
        df.address = df.address.astype('string')
        df.country = df.country.astype('category')
        df.country_code = df.country_code.astype('category')
        df.join_date = df.join_date.astype('datetime64[ns]')
        df.user_uuid = df.user_uuid.astype('string')

        df.drop_duplicates(inplace=True)
        
        return df
    
    def clean_card_data(self):
        df = DataExtractor().retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
        print(df.info())
        print(df.describe())
        df.replace('NULL', pd.NA, inplace=True)
        df.dropna(how='all', inplace=True)
        
        # Convert to string to handle NaN values
        df.card_number = df['card_number'].astype('string')

        # Filter rows where 'card_number' is numeric
        df['card_number'] = df['card_number'].str.replace('?','')
        df = df[df['card_number'].str.isnumeric()]


        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce') + pd.offsets.MonthEnd(0)
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], format='mixed', errors='coerce')


        df.card_number = df.card_number.astype('int64')
        df.card_provider = df.card_provider.astype('category')

        DatabaseConnector().upload_to_db(df, 'dim_card_details')

DataCleaning().clean_card_data()
