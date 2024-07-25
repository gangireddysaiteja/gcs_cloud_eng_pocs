from google.cloud import storage
from google.oauth2.credentials import Credentials
import json
import os
from datetime import datetime
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pandas as pd

class GCPStorageClient:
    def __init__(self, project_id, bucket_name, identity_json):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.identity = json.loads(identity_json)
        self.credentials = Credentials.from_authorized_user_info(self.identity)
        self.client = storage.Client(project=self.project_id, credentials=self.credentials)
        self.bucket = self.client.get_bucket(self.bucket_name)

    def upload_file(self, source_file_name, destination_blob_name):
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(f'File {source_file_name} uploaded to {destination_blob_name}.')

    def upload_files_from_directory(self, source_directory, destination_directory):
        # Get the current date
        current_date = datetime.now().strftime('%Y%m%d')
        for filename in os.listdir(source_directory):
            file_extension = os.path.splitext(filename)[1][1:]
            destination_directory_path = f'{destination_directory}{file_extension}/{current_date}'
            print(destination_directory_path)

            source_file_path = os.path.join(source_directory, filename)
            if os.path.isfile(source_file_path):
                destination_blob_name = f'{destination_directory_path}/{filename}'
                print(destination_blob_name)
                self.upload_file(source_file_path, destination_blob_name)
    def getsqlconnectionstring(self):
        connn_str = (
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=DESKTOP-TMIQ2ND\\SQLEXPRESS;"
            "Database=UseCase_Task;"
            "Uid=sa;"
            "Pwd=sa123;"
        )
        server = "DESKTOP-TMIQ2ND\\SQLEXPRESS"
        database = "UseCase_Task"
        Uid = "sa"
        pwd = "sa123"
        encoded_pwd = quote_plus(pwd)
        self.conn_str = f"mssql+pyodbc://{Uid}:{encoded_pwd}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    def readfromsql_loadintogcs(self,destination_blob_empdata):
        #conn_str = self.getsqlconnectionstring()
        engine = create_engine(self.conn_str)
        query = "select distinct * from (SELECT CAST(load_datetime AS DATE) AS load_date FROM emp_data ) as empdata order by load_date"
        loaddates_df = pd.read_sql_query(query, engine)
        print(loaddates_df)
        for i in range(len(loaddates_df)):
            load_date = loaddates_df.iloc[i]['load_date']
            query = f"SELECT emp_id,empname,emp_salary,deptno,CAST(load_datetime AS DATE) AS load_date FROM emp_data where CAST(load_datetime AS DATE) = '{load_date}'"
            emp_data_df = pd.read_sql_query(query, engine)
            os.listdir()

            file_path = f"D:\\GCPEngineer\\Python\\{load_date}\\"
            file_name ="emp_data.csv"
            if not os.path.exists(file_path):
                os.makedirs(file_path)
            filename = f'{file_path}\\{file_name}'
            destination_gcs_path = f'{destination_blob_empdata}{load_date}/emp_data.csv'
            emp_data_df.to_csv(filename, index=False)
            self.upload_file(filename, destination_gcs_path)
# Define the identity JSON and other parameters
identity_json = """{
    "account": "",
    "client_id": "764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com",
    "client_secret": "d-FL95Q19q7MQmFpd7hHD0Ty",
    "quota_project_id": "de-training-dev",
    "refresh_token": "1//04SYCyUiQkZ7tCgYIARAAGAQSNwF-L9Irys3v8y9-XsJfXVIHlp5iqZX0658NQOXzGipZ09IGcEnnUkpWvLHrSQx29oWPUzGAzYI",
    "type": "authorized_user",
    "universe_domain": "googleapis.com"
}"""
project_id = "de-training-dev"
bucket_name = 'dev-de-training'
source_file_name = 'D:\\GCPEngineer\\Python\\SalaryBand.xlsx'
destination_blob_name = 'data/RAW_ZONE/SalaryBand.xlsx'
source_files_path = 'D:\\GCPEngineer\\Python\\storage-files'
destination_blob_path ='data/flatflies/raw_'
destination_blob_empdata ='data/db/raw_data/emp_data/'
# Create an instance of the GCPStorageClient class
gcp_storage_client = GCPStorageClient(project_id, bucket_name, identity_json)

# Upload the file using the class method
# gcp_storage_client.upload_file(source_file_name, destination_blob_name)

# gcp_storage_client.upload_files_from_directory(source_files_path, destination_blob_path)

gcp_storage_client.getsqlconnectionstring()
#print(conn_str)
gcp_storage_client.readfromsql_loadintogcs(destination_blob_empdata)

