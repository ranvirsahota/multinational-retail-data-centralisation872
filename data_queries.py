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
                    SELECT
                        *
                    FROM dim_store_details
                '''
        print(self.data_extrc.read_rds_table(query, self.db_conn))

    