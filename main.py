from data_cleaning import DataCleaning

def test(word):
    print(f'hello{word}world')

if __name__ == "__main__":
    old_db_creds = './db_creds_aws.yaml'
    new_db_creds = './db_creds_local.yaml'

    data_clean = DataCleaning(old_db_creds, new_db_creds)
    # data_clean.MEATHO_NAME()
    # replace 'METHOD_NAME with table to be cleaned'
    # CHOICES:
        # clean_user_data()
        # clean_card_data()
        # clean_store_data()
        # clean_products_data()
        # clean_sales_data()
        # clean_orders_data()