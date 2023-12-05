from data_cleaning import *
from data_extraction import *

if __name__ == "__main__":
    old_db_creds = './db_creds_aws.yaml'
    new_db_creds = './db_creds_local.yaml'
    data_clean = DataCleaning(old_db_creds, new_db_creds)

    data_clean.foreign_keys_dim_tbl()