import requests
import logging
import os
import pandas as pd
import azure.functions as func
import datetime
import tempfile
from azure.identity import ClientSecretCredential
from azure.mgmt.storage import StorageManagementClient 
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import io
from azure.storage.blob import BlobServiceClient

# Define your Azure AD credentials
tenant_id = os.getenv('AR_TENANT_ID')
client_id = os.getenv('AR_CLIENT_ID')
client_secret = os.getenv('AR_APP_SECRET')

# Authenticate using the service principal
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

key_vault_url = 'https://kv-cd-databrein-d1.vault.azure.net/'
client = SecretClient(vault_url=key_vault_url, credential=credential)

# Function to retrieve secret from Key Vault
def get_secret(secret_name):
    secret = client.get_secret(secret_name)
    return secret.value

# Azure Data Lake Storage settings
account_name = 'dlscddatabreind1'
account_key = get_secret('dls-databrein-d1-v2')  # GET FROM KV?
container_name = 'insta-csv-files'
folder_name = 'Insta_insights'  # Specify your desired folder
current_date = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
csv_filename = f'Insta_insights_{current_date}.csv'

def store_in_dls(account_name, account_key, folder_name, file_name, data):
        # Initialize BlobServiceClient with your Azure Storage account credentials
        blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net",
                                                credential=account_key)

        # Get a BlobClient for your Data Lake Storage folder
        blob_client = blob_service_client.get_blob_client(container=container_name,
                                                        blob=f"{folder_name}/{file_name}")

        blob_client.upload_blob(data, overwrite = True)

        return print(f'{file_name} was uploaded to the data lake storage')
    

def access_file_from_adls(account_name, account_key, container_name, folder_name, file_name):
    try:
        # Initialize BlobServiceClient with your Azure Storage account credentials
        logging.info("Initializing BlobServiceClient.")
        blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net",
                                                credential=account_key)

        # Get a BlobClient for your Data Lake Storage file
        logging.info("Getting BlobClient for the file.")
        blob_client = blob_service_client.get_blob_client(container=container_name,
                                                          blob=f"{folder_name}/{file_name}")

        # Check if the blob exists
        logging.info("Checking if blob exists.")
        if not blob_client.exists():
            logging.error(f"Blob {file_name} does not exist in the container {container_name}.")
            return None

        # Download the file content as bytes
        logging.info("Downloading blob content.")
        download_stream = blob_client.download_blob()

        # Read the content in chunks
        file_content = bytearray()
        for chunk in download_stream.chunks():
            file_content.extend(chunk)

        if not file_content:
            logging.error("Failed to download blob content or content is empty.")
            return None

        # Convert the byte content to a stream and read it as a pandas DataFrame
        logging.info("Reading content as a pandas DataFrame.")
        file_stream = io.BytesIO(file_content)
        df = pd.read_csv(file_stream, sep = ';')

        return df

    except Exception as e:
        logging.error(f"Failed to read the CSV file: {e}")
        return None
    
def create_temp_csv_string(df):
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index = False, sep = ';')
        csv_data = csv_buffer.getvalue()
        return csv_data

from azure.storage.blob import BlobServiceClient

def delete_file_from_adls(account_name, account_key, container_name, folder_name, file_name):
    try:
        # Initialize BlobServiceClient
        blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net",
                                                credential=account_key)

        # Get a BlobClient for the file
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{folder_name}/{file_name}")

        # Delete the file
        blob_client.delete_blob()

        print(f"File deleted successfully: {folder_name}/{file_name}")

    except Exception as e:
        print(f"Error deleting the file: {e}")


### Perform the merging and archiving of the files
def main(req: func.HttpRequest) -> func.HttpResponse:
    
    # Retrieve the necessary files
    bulk_file = 'Insta_insights.csv'
    daily_file = csv_filename
    bulk_df = access_file_from_adls(account_name, account_key, container_name, folder_name, bulk_file)
    daily_df = access_file_from_adls(account_name, account_key, container_name, folder_name, daily_file)
    
    # Remove empty rows from daily_df
    columns_to_check = [6, 7, 8]  # Assuming col1 and col2 by their index positions
    # Remove rows with empty values in specified columns
    daily_df = daily_df.dropna(subset=daily_df.columns[columns_to_check])
    
    # Remove rows from bulk_df that are in daily_df
    bulk_df_filtered = bulk_df[~bulk_df['date'].isin(daily_df['date'])]
    # Merge the dataframes
    merged_df = pd.concat([bulk_df_filtered, daily_df], ignore_index=True)

    # Write merged data back to bulk data CSV.
    bulk_csv_data = create_temp_csv_string(merged_df)  
    store_in_dls(account_name, account_key, folder_name, bulk_file, bulk_csv_data)
    # Write daily data to archive.
    daily_csv_data = create_temp_csv_string(daily_df)
    store_in_dls(account_name, account_key, 'Archief', daily_file, daily_csv_data)
    
    # Remove the daily file from its orignal folder (since it has been archived)
    delete_file_from_adls(account_name, account_key, container_name, folder_name, daily_file)
        
    return func.HttpResponse(f"Files {bulk_file} and {csv_filename} were merged and archived")
