from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd
import numpy as np
import sqlalchemy, re
class DataCleaning:
    '''
    Parameters:
    ----------


    Attributes:
    ----------


    Methods:
    -------
    
    '''

    def __init__(self, old_db_creds, new_db_creds) -> None:
        self.old_db = DatabaseConnector(old_db_creds)
        self.new_db= DatabaseConnector(new_db_creds)

    def _set_weight_class(self, weight:int):
        if weight < 2:
            return 'Light'
        elif weight >= 2 and weight < 40:
            return 'Mid_Sized'
        elif weight >= 40 and weight < 140:
            return 'Heavy'
        else:
            return 'Truck_Required'
    
    def _convert_product_weights(self, product_weight:str) -> float:
        weight_num_match = re.search(r'\d+(\.\d+)*', product_weight)
        if not weight_num_match:
            return np.nan
        weight_num = float(weight_num_match.group())
        if ('kg' not in product_weight):
            weight_num /= 1000
        return weight_num

    def clean_user_data(self):
        data_extrc = DataExtractor()
        users_df = data_extrc.read_rds_table('legacy_users')
        users_df.set_index('index', inplace=True)
        # Chanage 'GGB' to 'GB', all rows with 'GGB' are in the UK
        users_df['country_code'] = users_df['country_code'].replace('GGB', 'GB')
        # remove all rows with null values or errornous data, 
        # Can be find by find all country codes not two characters
        users_df = users_df[users_df['country_code'].str.len() == 2]
        users_df['date_of_birth'] = pd.to_datetime(users_df['date_of_birth'], format='mixed', errors='coerce')
        users_df['join_date'] = pd.to_datetime(users_df['join_date'], format='mixed', errors='coerce')
        users_df.date_of_birth = users_df.date_of_birth.astype('datetime64[ns]')
        users_df.join_date = users_df.join_date.astype('datetime64[ns]')
        DatabaseConnector().upload_to_db(
            sql_types = {
                'user_uuid' : sqlalchemy.types.UUID,
                'first_name' : sqlalchemy.types.VARCHAR(255),
                'last_name' : sqlalchemy.types.VARCHAR(255),
                'date_of_birth' : sqlalchemy.types.DATE,
                'country_code' : sqlalchemy.types.VARCHAR(2),
                'join_date' : sqlalchemy.types.DATE
            },
            df = users_df,
            tbl_name = 'dim_users_table',
            primary_key = 'user_uuid'
        )

    def clean_card_data(self):
        cards_df = DataExtractor().retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
        cards_df['expiry_date'].replace('NULL', pd.NaT, inplace = True)
        cards_df['date_payment_confirmed'].replace('NULL', pd.NaT, inplace = True)
        cards_df.dropna(how='all', inplace=True)
        cards_df['card_number'] = cards_df['card_number'].astype('string').str.replace('?','')
        cards_df = cards_df[cards_df['card_number'].str.isnumeric()]
        print(cards_df [~cards_df['date_payment_confirmed'].astype('string').str.contains('-')])
        #cards_df['expiry_date'] = pd.to_datetime(cards_df['expiry_date'], format='%m/%y', errors='coerce') + pd.offsets.MonthEnd(0)
        #cards_df['date_payment_confirmed'] = pd.to_datetime(cards_df['date_payment_confirmed'], format='mixed', errors='coerce')
        self.new_db.upload_to_db(
            sql_types = {
                'card_number' : sqlalchemy.types.VARCHAR(cards_df['card_number'].astype('string').str.len().max()),
                'expiry_date' : sqlalchemy.types.VARCHAR(cards_df['expiry_date'].astype('string').str.len().max()),
                'date_payment_confirmed' : sqlalchemy.types.DATE,
            },
            df = cards_df,
            tbl_name = 'dim_card_details',
            primary_key = 'card_number'
        )

    def clean_store_data(self):
        stores_df = DataExtractor().retrieve_stores_data(endpoint_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/',
                                     header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})
        stores_df.set_index('index', inplace=True)
        stores_df.drop('lat', axis = 1, inplace=True)
        stores_df = stores_df[stores_df['country_code'].isin(['US', 'GB', 'DE'])]
        stores_df['address'] = stores_df['address'].str.replace('\n', ', ')
        stores_df['staff_numbers'] = stores_df['staff_numbers'].str.extract('(\\d+)')
        stores_df['opening_date'] = pd.to_datetime(stores_df['opening_date'], format='mixed', errors='raise')
        stores_df['continent'] = stores_df['continent'].str.removeprefix('ee')
        stores_df.at[0, ['address', 'longitude', 'locality', 'latitude']] = ['N/A', np.nan, 'N/A', np.nan]
        stores_df.longitude = stores_df.longitude.astype('float64')
        stores_df.staff_numbers = stores_df.staff_numbers.astype('int64')
        stores_df.latitude = stores_df.latitude.astype('float64')

        DatabaseConnector().upload_to_db(
            sql_types={
                'store_code' : sqlalchemy.types.VARCHAR(stores_df['store_code'].astype('string').str.len().max()),
                'longitude' : sqlalchemy.types.FLOAT,
                'locality' : sqlalchemy.types.VARCHAR(255),
                'staff_numbers' : sqlalchemy.types.SMALLINT,
                'opening_date' : sqlalchemy.types.DATE,
                'store_type' : sqlalchemy.types.VARCHAR(255),
                'latitude' : sqlalchemy.types.FLOAT,
                'country_code' : sqlalchemy.types.VARCHAR(2),
                'continent' : sqlalchemy.types.VARCHAR(255)
            },
            df = stores_df, 
            tbl_name = 'dim_store_details',
            primary_key = 'store_code'
        )

    def clean_products_data(self):
        products_df = DataExtractor().extract_from_s3('s3://data-handling-public/products.csv')
        products_df['weight'] = products_df['weight'].apply(self._convert_product_weights)
        products_df.set_index('index', inplace=True)

        products_df = products_df[~products_df['category'].isin(['S1YB74MLMJ', 'C3NCA2CL35', 'WVPMHZP59U', 'NULL'])]
        products_df['date_added'] = pd.to_datetime(products_df['date_added'], format='mixed', errors='raise')
        
        # only convert non-strings to ensure erronous data removed/resolved
        products_df['weight'] = products_df['weight'].astype('float')
        products_df['date_added'] = products_df['date_added'].astype('datetime64[ns]')
        products_df['weight_class'] = products_df['weight'].apply(self._set_weight_class)

        products_df.rename(columns={'removed' : 'still_available'}, inplace=True)
        products_df['still_available'] = products_df['still_available'].apply(lambda status: True if status == 'Still_avaliable' else False)
        self.new_db.upload_to_db(
            sql_types = {
                'product_code' : sqlalchemy.types.VARCHAR(products_df['product_code'].astype('string').str.len().max()),
                'index' : sqlalchemy.types.INTEGER,
                'weight' : sqlalchemy.types.FLOAT,
                'category' : sqlalchemy.types.VARCHAR(products_df['category'].astype('string').str.len().max()),
                'EAN' : sqlalchemy.types.VARCHAR(products_df['EAN'].astype('string').str.len().max()),
                'date_added' : sqlalchemy.types.DATE,
                'uuid' : sqlalchemy.types.UUID,
                'still_available' : sqlalchemy.types.BOOLEAN,
                'weight_class' : sqlalchemy.types.VARCHAR(products_df['weight_class'].astype('string').str.len().max())
            },
            df = products_df,
            tbl_name = 'dim_products',
            primary_key = 'product_code'
        )
        query = 'UPDATE dim_products SET product_price = SUBSTRING(product_price FROM 2);'
        print(self.new_db.execute_query(query))

        query = 'ALTER TABLE dim_products ALTER product_price TYPE FLOAT USING (trim(product_price)::FLOAT);'
        print(self.new_db.execute_query(query))
        
    def clean_sales_data(self):
        sales_df = DataExtractor().get_sales_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
        sales_df = sales_df[sales_df['time_period'].isin(['Evening', 'Morning', 'Midday', 'Late_Hours'])]
        tbl_name = 'dim_date_times'
        self.new_db.upload_to_db(
            sql_types = {
                'date_uuid' : sqlalchemy.types.UUID,
                'time_period' : sqlalchemy.types.VARCHAR(sales_df['time_period'].astype('string').str.len().max()),
                'timestamp' : sqlalchemy.types.TIME,
                'month' : sqlalchemy.types.VARCHAR(sales_df['month'].astype('string').str.len().max()),
                'year' : sqlalchemy.types.VARCHAR(sales_df['year'].astype('string').str.len().max()),
                'day' : sqlalchemy.types.VARCHAR(sales_df['day'].astype('string').str.len().max())
                },
            df = sales_df,
            tbl_name = 'dim_date_times',
            primary_key = 'date_uuid'
        )
    
    def clean_orders_data(self):
        query = 'SELECT * FROM orders_table;'
        orders_df = DataExtractor().read_rds_table(query, self.old_db)
        orders_df.set_index('index', inplace=True)
        orders_df.drop(columns=['level_0','first_name', 'last_name', '1'], axis=1, inplace=True)
        
        self.new_db.upload_to_db(
            sql_types={
                'date_uuid' : sqlalchemy.types.UUID,
                'user_uuid' : sqlalchemy.types.UUID,
                'card_number': sqlalchemy.types.VARCHAR(orders_df['card_number'].astype(str).str.len().max()),
                'store_code': sqlalchemy.types.VARCHAR(orders_df['store_code'].astype(str).str.len().max()),
                'product_code': sqlalchemy.types.VARCHAR(orders_df['product_code'].astype(str).str.len().max()),
                'product_quantity': sqlalchemy.types.SMALLINT
            },
            df = orders_df,
            tbl_name = 'orders_table'
        )
        foreign_keys = [
            ['dim_date_times', 'date_uuid'],
            ['dim_users_table', 'user_uuid'],
            ['dim_card_details', 'card_number'],
            ['dim_store_details', 'store_code'],
            ['dim_products', 'product_code'],
        ]
        for tbl_name, col_name in foreign_keys:
            query = (f'ALTER TABLE orders_table '+
                        f'ADD CONSTRAINT fk_{col_name} '+
                            f'FOREIGN KEY ({col_name}) ' + 
                                f'REFERENCES {tbl_name}({col_name});')
            result = self.new_db.execute_query(query)
            print(result)