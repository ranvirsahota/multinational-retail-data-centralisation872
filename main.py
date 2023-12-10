from data_cleaning import DataCleaning

def test(word):
    print(f'hello{word}world')

if __name__ == "__main__":
    old_db_creds = './db_creds_aws.yaml'
    new_db_creds = './db_creds_local.yaml'

    data_clean = DataCleaning(old_db_creds, new_db_creds)
    data_clean.clean_orders_data()
