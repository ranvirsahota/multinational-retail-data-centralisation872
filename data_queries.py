from data_cleaning import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd
from IPython.display import display, HTML
class DataQueries():
    
    def __init__(self, db_creds) -> None:
        self.db_conn = DatabaseConnector(db_creds)
        self.data_extrc = DataExtractor()

    
    def store_count_by_country(self):
        query = '''
                    SELECT 
                        country_code, 
                        COUNT(country_code) AS store_count
                    FROM dim_store_details
                    GROUP BY country_code
                    ORDER BY count DESC;
                  '''
        print(self.data_extrc.read_rds_table(query, self.db_conn))

    def store_count_by_locality(self):
        query = '''
                    SELECT 
                        locality, 
                        COUNT(locality) AS store_count
                    FROM dim_store_details
                    GROUP BY locality
                    ORDER BY store_count DESC
                    LIMIT 7;
                  '''
        print(self.data_extrc.read_rds_table(query, self.db_conn))

    def most_sales_by_month(self):
        query = '''
                    SELECT ROUND(
                                    CAST(SUM(dim_products.product_price * orders_table.product_quantity) AS numeric), 2
                                ) AS total_sales,
                        dim_date_times.month
                    FROM orders_table
                    INNER JOIN dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
                    INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code
                    GROUP BY dim_date_times.month
                    ORDER BY total_sales DESC
                    LIMIT 6;
                '''
        print(self.data_extrc.read_rds_table(query, self.db_conn))
    
    def sales_onlive_vs_offline(self):
        query = '''
                    SELECT
                            COUNT(date_uuid) AS number_of_sales,
                            SUM(orders.product_quantity),
                            CASE
                                WHEN store_type IN ('Web Portal')
                                    THEN 'Web'
                                WHEN store_type IN ('Super Store', 'Local', 'Outlet', 'Mall Kiosk')
                                    THEN 'Offline'
                            END AS location
                    FROM orders_table AS orders
                    INNER JOIN dim_store_details AS stores ON stores.store_code = orders.store_code
                    GROUP BY location
                    ORDER BY number_of_sales ASC
                '''
        print(self.data_extrc.read_rds_table(query, self.db_conn))

    