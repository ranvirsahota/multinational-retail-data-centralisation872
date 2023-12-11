import yaml, requests, sqlalchemy, psycopg2
import pandas as pd


class DatabaseConnector:
    '''
    Parameters:
    ----------
    file_path_creds: str
        The file path to the file containing credentials to access a database in string format
    Attributes:
    ----------
    _file_path_creds: str
        Applied file path as an attribute for ease of access arround classes and to provide information in error handling
    _engine
        instance of SQLAlchemy engine which provides access to database to r/w queries

    Methods:
    -------
    __print_error(statements)
        to provide a consistent approach in displaying error messages
    read_db_creds()
        Will read any any YAML file and return the result as directory
    init_db_engine()
        Creates and reutrns an sqlalchemy engine
    execute_query(query:str)
        Executes any query and returns the results
    upload_to_db(df:pd.DataFrame, tbl_name:str, sql_types:dict, primary_key:str= None)
        Uploads dataframe to database
    '''

    def __init__(self, file_path_creds) -> None:
        self._file_path_creds = file_path_creds
        self._engine = self._init_db_engine()

    def _print_error(self, statements):
        """
        Print error messages in a concise and consistent format.

        Parameters:
        ----------
        statements : str or list
            A single error statement or a list of error statements.
        """
        print('*' * 50)
        if (type(statements) is list):
            for statment in statements:
                print(statment)
        else:
            print(statements)
        print('*' * 50)

    def _read_db_creds(self) -> any:
        '''
        Read database credentials from a YAML file.

        Returns:
        -------
        dict
            A dictionary containing database credentials.
        '''
        try:
            with open(self._file_path_creds, 'r') as file:
                return yaml.safe_load(file)
        except OSError as err:
            self._print_error(f'OSError with open method: {err}')
            raise
        except yaml.YAMLError as err:
            if hasattr(err, 'problem_mark'):
                mark = err.problem_mark
                self._print_error(f'Error position: {mark.line+1}:{mark.column+1}')
                raise
        except Exception as err:
            print('Unexpected error has occurred during opening and parsing of YAML file, in read_db_creds():', err)
            raise
    
    def _init_db_engine(self) -> sqlalchemy.engine:
        '''
        Initialize and return an SQLAlchemy engine for connecting to the database.

        Returns:
        -------
        sqlalchemy.engine
            An SQLAlchemy engine instance.
        '''
        creds = self._read_db_creds()
        try:
            user = creds['RDS_USER']
            passwd = creds['RDS_PASSWORD']
            host = creds['RDS_HOST']
            port = creds['RDS_PORT']
            db_name = creds['RDS_DATABASE'] 
            return sqlalchemy.create_engine(f"postgresql://{user}:{passwd}@{host}:{port}/{db_name}")
        except KeyError as err:
            self._print_error([f'File at this path {self._file_path_creds} does not conform to credential standard, look at readme to know credential layout',
                              f'Missing Key is: {err.args[0]}'])
            raise
        except sqlalchemy.exc.OperationalError as err:
            self._print_error([f'An error occurred when connection to database was initiated, error: {err}',
                            f'Ensure the credentials are correct and you have access to databse'])
            raise 
        except Exception as err:
            self._print_error(f'Unexpected error occurred during sqlalchemy.engine initiation and connection: {err}')
            raise

    def execute_query(self, query:str) -> any:
        '''
        Execute an SQL query on the connected database.

        Parameters:
        ----------
        query : str
            The SQL query to be executed.

        Returns:
        -------
        ResultProxy
            The result proxy object containing the results of the query. 
        '''
        try:
            with self._engine.begin() as engine_conn:
                result = engine_conn.execute(sqlalchemy.TextClause(query))
                print(result)
                return result
        except sqlalchemy.exc.IntegrityError as err:
            self._print_error(f'SQLAlchemy IntegrityError: {err}')
            raise
        except sqlalchemy.exc.ProgrammingError as err:
            self._print_error([
                f'SQLAlchemyError raised at execute_query()',
                f'Error is sytax related or reference does not exist: {err}'])
            raise
        except err:
            self._print_error(f'Unexpected error: {err}')
            raise            
    
    
    def upload_to_db(self, df:pd.DataFrame, tbl_name:str, sql_types:dict, primary_key:str= None):
        '''
        Upload a DataFrame to the database.

        Parameters:
        ----------
        df : pd.DataFrame
            The DataFrame to be uploaded.
        tbl_name : str
            The name of the table in the database.
        sql_types : dict
            A dictionary specifying the data types for each column in the DataFrame.
        primary_key : str, optional
            The primary key column name or the table.

        Raises:
        -------
        Exception
            If an unexpected error occurs during the upload process.
        '''
        try:
            with self._engine.connect() as engine_conn:
                self.execute_query(f'DROP TABLE IF EXISTS {tbl_name} CASCADE;')
                result = df.to_sql(name=tbl_name, con=engine_conn, index=True, if_exists='replace', dtype=sql_types)
                if (result.is_integer):
                    print(f'DataFrame Uploaded, records affected: {result}')
                    if primary_key != None:
                        self.execute_query(F'ALTER TABLE {tbl_name} ADD PRIMARY KEY ({primary_key})')
                        print(f'PRIMARY KEY {primary_key} CREATED ON {tbl_name}')
                else:
                    print(f'DataFrame not uploaded, the error that occured: {result}')

        except Exception as err:
            self._print_error(f'Unexpected error : {err}')
            raise