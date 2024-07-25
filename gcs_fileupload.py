from google.cloud import storage
from google.oauth2.credentials import Credentials
import json

identity = json.loads("""{
	        "account": "",
	        "client_id": "764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com",
	        "client_secret": "d-FL95Q19q7MQmFpd7hHD0Ty",
	        "quota_project_id": "de-training-dev",
	        "refresh_token": "1//04SYCyUiQkZ7tCgYIARAAGAQSNwF-L9Irys3v8y9-XsJfXVIHlp5iqZX0658NQOXzGipZ09IGcEnnUkpWvLHrSQx29oWPUzGAzYI",
	        "type": "authorized_user",
	        "universe_domain": "googleapis.com"
            }""")
# Project and bucket details
project_id = "de-training-dev"
bucket_name = 'dev-de-training'
source_file_name = 'D:\\GCPEngineer\\Python\\SalaryBand.xlsx'
destination_blob_name = 'data/RAW_ZONE/SalaryBand.xlsx'
gcp_credentials = Credentials.from_authorized_user_info(identity)


# Create a storage client
gcsClient = storage.Client(project=project_id, credentials=gcp_credentials)

# Get the bucket
gcsBucket = gcsClient.get_bucket(bucket_name)

# Create a blob object
gcs_file = gcsBucket.blob(destination_blob_name)

# Upload the file
gcs_file.upload_from_filename(source_file_name)

print('File uploaded')


