import requests
import logging
import os
import sys
import netCDF4 as nc
import pandas as pd
import numpy as np
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pyodbc
from get_secret import get_secret
from azure.identity import ClientSecretCredential
from azure.mgmt.storage import StorageManagementClient 
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from io import StringIO
import re 
 
# Define your Azure AD credentials
tenant_id = os.getenv('AR_TENANT_ID')
client_id = os.getenv('AR_CLIENT_ID')
client_secret = os.getenv('AR_APP_SECRET')
# subscription_id = '37e012c1-8275-4327-9d0f-cfae380fe36d'

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
container_name = 'cd-csv-files'
folder_name = 'CD_Backend_API'  # Specify your desired folder
post_key = get_secret('CD-API-POST-Key')

def main(req: func.HttpRequest) -> func.HttpResponse:
    ## Call the API and retrieve data
    headers = {
        'viewcsv': post_key,
        #'Content-Type': 'application/json'
    }
    
    response = requests.post(headers=headers, url = "https://customdecks.be/admin/plugins/besteldata_corne_rook.php?u=ernoc&p=poiuy")
    data = response.text
    
    if data is not None:
      logging.info('API call gemaakt')
    else:
      logging.info('API call mislukt')

    ## Verwerk de text string voor nadere uitsplitsing naar kolommen
    datetime_pattern = r'(\d{2}-\d{2}-\d{4}) (\d{2}:\d{2}:\d{2})'
    time_pattern     = r'\d{2}:\d{2}:\d{2}'
    def reverse_datetime(match):
        # Extract date and time components from the match object
        date_part = match.group(1)
        time_part = match.group(2)
        
        # Reverse the order of date and time components and return
        return f"{time_part} {date_part}"
    
    # Use re.sub() to replace date-time patterns with reversed order
    reversed_data= re.sub(datetime_pattern, reverse_datetime, data)
    rows = re.split(time_pattern, reversed_data)
    
    # Extraheer de headers
    headers = str(rows[0])
    header_index = headers.find('\n')
    headers = headers[:230]
    
    # Skip de eerste rij omdat deze de headers bevat
    data_without_header = rows[1:]
    rows = data_without_header
    
    i = 1
    print(headers)
    for row in rows:
        #print(row)
        #print("BREAK")
        i += 1

    # Split the semicolon separated text into columns
    csv_filename = 'CustomDecksData_Backend.csv'
    
    split_data = [row.split(';') for row in rows]
    column_headers = headers.split(';')
    column_amount  = len(column_headers)
    
    # Create DataFrame from the split data
    df = pd.DataFrame(split_data) 
    df = df.iloc[:,:column_amount]
    df.columns = column_headers
    df = df.map(lambda x: x.strip('"') if isinstance(x, str) else x)
    df.rename(columns=lambda x: x.strip('"'), inplace=True)
    df.replace('\n', ':', regex=True, inplace=True)
    df.replace('\r', '-', regex=True, inplace=True)

    

    ## Store the csv in the dls
    # Initialize BlobServiceClient with your Azure Storage account credentials
    blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net",
                                            credential=account_key)
    logging.info('Initialized BlobServiceClient')

    # Get a BlobClient for your Data Lake Storage folder
    blob_client = blob_service_client.get_blob_client(container=container_name,
                                                      blob=f"{folder_name}/{csv_filename}")

    # Setup IO string to prevent local storage
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index = False, sep = ';')
    csv_data = csv_buffer.getvalue()

    blob_client.upload_blob(csv_data, overwrite = True)

    # Create the csv file # Uncomment this when using the "with open() lines
    # df.to_csv(csv_filename, index=False, sep = ';')
    
    # Upload the downloaded file to Data Lake Storage
    #with open(csv_filename, "rb") as data:
    #    blob_client.upload_blob(data, overwrite = True)
    #logging.info('Uploaded the file')

    # Optionally, delete the local downloaded file
    #os.remove(csv_filename) 
      
    return func.HttpResponse(f"{csv_filename} was uploaded successfully to {container_name}/{folder_name}", status_code = 200)
