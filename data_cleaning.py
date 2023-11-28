from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd
import numpy as np
import re
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
        df['card_provider'].replace('NULL', None, inplace = True)
        df['card_number'].replace('NULL', np.nan, inplace = True)
        df['expiry_date'].replace('NULL', pd.NaT, inplace = True)
        df['date_payment_confirmed'].replace('NULL', pd.NaT, inplace = True)
        df.dropna(how='all', inplace=True)
        
        # Filter rows where 'card_number' is numeric
        df.card_number = df['card_number'].astype('string')
        df['card_number'] = df['card_number'].str.replace('?','')
        df = df[df['card_number'].str.isnumeric()]


        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce') + pd.offsets.MonthEnd(0)
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], format='mixed', errors='coerce')


        df.card_number = df.card_number.astype('int64')
        df.card_provider = df.card_provider.astype('category')

        DatabaseConnector().upload_to_db(df, 'dim_card_details')
    
    def called_clean_store_data(self):
        stores_df = DataExtractor().retrieve_stores_data(endpoint_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/',
                                     header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})
        stores_df.set_index('index', inplace=True)
        stores_df.drop('lat', axis = 1, inplace=True)
        stores_df = stores_df[stores_df['country_code'].isin(['US', 'GB', 'DE'])]
        stores_df['address'] = stores_df['address'].str.replace('\n', ', ')
        stores_df.loc[0, ['address', 'longitude', 'locality']] = [pd.NA, np.nan, np.nan]
        stores_df['staff_numbers'] = stores_df['staff_numbers'].str.extract('(\\d+)')
        stores_df['opening_date'] = pd.to_datetime(stores_df['opening_date'], format='mixed', errors='raise')
        stores_df['continent'] = stores_df['continent'].str.removeprefix('ee')

        stores_df.address = stores_df.address.astype('string')
        stores_df.longitude = stores_df.longitude.astype('float64')
        stores_df.locality = stores_df.locality.astype('category')
        stores_df.store_code = stores_df.store_code.astype('category')
        stores_df.staff_numbers = stores_df.staff_numbers.astype('int64')
        stores_df.store_type = stores_df.store_type.astype('category')
        stores_df.latitude = stores_df.latitude.astype('float64')
        stores_df.country_code = stores_df.country_code.astype('category')
        stores_df.continent = stores_df.continent.astype('category')

        DatabaseConnector().upload_to_db(stores_df, 'dim_store_details')

    def convert_product_weights(self, products_df:pd.DataFrame):
        for index, weight in enumerate(products_df['weight']):
            weight_num = re.search(r'\d+(\.\d+)*', weight)
            if (weight_num == None):
                products_df['weight'][index] = np.nan
                continue
            weight_num = float(weight_num.group())
            if ('kg' not in weight):
                weight_num /= 1000
            products_df['weight'][index] = weight_num



    def clean_products_data(self):
        products_df = DataExtractor().extract_from_s3('s3://data-handling-public/products.csv')

        self.convert_product_weights(products_df)
        products_df = products_df[~products_df['category'].isin(['S1YB74MLMJ', 'C3NCA2CL35', 'WVPMHZP59U', 'NULL'])]
        products_df['date_added'] = pd.to_datetime(products_df['date_added'], format='mixed', errors='raise')

        products_df['index'] = products_df['index'].astype('int64')
        products_df['product_name'] = products_df['product_name'].astype('string')
        products_df['product_price'] = products_df['product_price'].astype('string')
        products_df['weight'] = products_df['weight'].astype('float')
        products_df['category'] = products_df['category'].astype('category')
        products_df['EAN'] = products_df['EAN'].astype('int64')
        products_df['date_added'] = products_df['date_added'].astype('datetime64[ns]')
        products_df['uuid'] = products_df['uuid'].astype('string')
        products_df['removed'] = products_df['removed'].astype('category')
        products_df['product_code'] = products_df['product_code'].astype('string')

        DatabaseConnector().upload_to_db(products_df, 'dim_products')

    def clean_orders_data(self):
        orders_df = DataExtractor().read_rds_table('orders_table')
        orders_df.set_index('index', inplace=True)
        print(orders_df)
        orders_df.drop(columns=['level_0','first_name', 'last_name', '1'], axis=1, inplace=True)
        orders_df['date_uuid'].astype('string')
        orders_df[['user_uuid', 'card_number',
                   'store_code', 'product_code']] = orders_df[['user_uuid', 'card_number',
                                                               'store_code', 'product_code']].astype('category')
        
        DatabaseConnector().upload_to_db(orders_df, 'orders_table')

    def clean_sales_data(self):
        try:
            sales_df = DataExtractor().get_sales_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
            sales_df = sales_df[sales_df['time_period'].isin(['Evening', 'Morning', 'Midday', 'Late_Hours'])]         
            sales_df['date_uuid'] = sales_df['date_uuid'].astype('string')
            sales_df[['time_period', 'day', 'month', 'year']] = sales_df[['time_period', 'day', 'month', 'year']].astype('category')
            DatabaseConnector().upload_to_db(sales_df, 'dim_date_times')
        except Exception as e:
            print(e)
