import yaml, requests, sqlalchemy
import pandas as pd

from psycopg2 import OperationalError


class DatabaseConnector:
    '''
    Parameters:
    ----------

    Attributes:
    ----------

    Methods:
    -------
    read_db_creds()
        Will read any any YAML file and return the result as directory
    init_db_engine()
        Creates and reutrns an sqlalchemy engine
    '''
    
    def connect_local_db(self):
        return self.init_db_engine('./db_creds_local.yaml')

    def connect_aws_db(self):
        return self.init_db_engine('./db_creds_awss.yaml')


    def _read_db_creds(self, file_path):
        try:
            with open(file_path, 'r') as file:
                return yaml.safe_load(file)
        except OSError as err:
            print('OSError with open method:', err)
            raise
        except yaml.YAMLError as err:
            if hasattr(err, 'problem_mark'):
                mark = err.problem_mark
                print("Error position: (%s:%s)" % (mark.line+1, mark.column+1))
                raise
        except Exception as err:
            print('Unexpected error has occurred during opening and parsing of YAML file, in read_db_creds():', err)
            raise
    
    def _init_db_engine(self, file_path) -> sqlalchemy.engine:
        creds = self.read_db_creds(file_path)
        try:
            user = creds['RDS_USER']
            passwd = creds['RDS_PASSWORD']
            host = creds['RDS_HOST']
            port = creds['RDS_PORT']
            db_name = creds['RDS_DATABASE'] 
            return sqlalchemy.create_engine(f"postgresql://{user}:{'dd'}@{host}:{port}/{db_name}").connect()
        except KeyError as err:
            self.print_error([f'File at this path {file_path} does not conform to credential standard, look at readme to know credential layout',
                              f'Missing Key is: {err.args[0]}'
                              ])
            raise
        except sqlalchemy.exc.OperationalError as err:
            self.print_error([f'An error occurred when connection to database was initiated, error: {err}',
                              f'Ensure the credentials at {file_path} are correct and you have access to databse'])
            raise
        except Exception as err:
            self.print_error(f'Unexpected error occurred during sqlalchemy.engine initiation and connection: {err}')
            raise

    def upload_to_db(self, file_path, df:pd.DataFrame, tbl_name:str):
        try:
            with self.init_db_engine(file_path) as conn:
                result = df.to_sql(name=tbl_name, con=conn, index=True, if_exists='replace')
                if (result.is_integer):
                    print(f'DataFrame Uploaded, records affected: {result}')
                else:
                    print(f'DataFrame not uploaded, the error that occured: {result}')
                    
        except Exception as err:
            self.print_error(f'Unexpected error occurred during sqlalchemy.engine initiation and connection: {err}')
            raise            
    
    def get_response(self, url:str):
        response = requests.get(url)
        if response.status_code == 200:
            print(f'Success: {response}')
            return response
        else:
            raise Exception(f'Error Occured: {response}')
        
    def print_error(self, statements):
        print('*' * 50)
        for statment in statements:
            print(statment)
        print('*' * 50)
        