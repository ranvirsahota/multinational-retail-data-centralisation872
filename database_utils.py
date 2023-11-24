import yaml
import pandas as pd
from sqlalchemy import create_engine
class DatabaseConnector:
    '''
    Parameters:
    ----------

    Attributes:
    ----------

    Methods:
    -------
    read_db_creds()
        Will read credentials from 'db_creds.yaml' file and return a directory of the credentials
    init_db_engine()
        Will read credentials from read_db_creds and initalise and reutrn an sqlalchemy database engine
    '''

    def read_db_creds(self, file):
        with open(file, 'r') as file:
            return yaml.load(file, Loader=yaml.SafeLoader)

    def init_db_engine(self):
        creds = self.read_db_creds('./db_creds_aws.yaml')
        return create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
    
    def upload_to_db(self, df:pd.DataFrame, tbl_name:str):
        creds = self.read_db_creds('./db_creds_local.yaml')
        with create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:" +
                           f"{creds['RDS_PORT']}/{creds['RDS_DATABASE']}").connect() as conn:
            try:
                print(df.to_sql(name=tbl_name, con=conn, index=True, if_exists='replace'))
            except Exception as e:
                print(f'Error uploading DataFrame to database: {e}')
