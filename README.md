# multinational-retail-data-centralisation872

## Table Of Contents
1. [Description Of Project](#description-of-project)
2. [Installation Instructions](#installation-instructions)
3. [Usage Instructions](#usage-instructions)
4. [File Structure Of The Project](#file-structure-of-the-project)
5. [License Information](#license-information)

## Description Of Project
### Project Scneario
You work for a multinational company that sells various goods across the globe.<br>
Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.<br>
In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location.<br>
Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.<br>
You will then query the database to get up-to-date metrics for the business.
### Learened So Far
Reinforced knowledge of 
- regular expression
- sqlalchemey
- pandas
- web apis 

## Installation Instructions
To run queries on a database download:
    - data_extraction.py
    - database_utils.py
    - data_queries.ipynb
To clean data from an old database to upload to a new one download (all files):
    - data_cleaning.py
    - data_extraction.py
    - database_utils.py
    - main.py
## Usage Instructions
Replace 'METHOD_NAME' with chosen method. For DataCleaning methods only no arguments are required for method calls. Then execute main.py, done in Linux:
```
python main.py
```
## File Structure Of The Project
    multinational_retail_repo
        |- .gitignore
        |- data_cleaning.py
        |- data_extraction.py
        |- database_utils.py
        |- db_creds_aws.yaml
        |- db_creds_local.yaml
        |- README.md
        |- s3_products.csv
## License Information
