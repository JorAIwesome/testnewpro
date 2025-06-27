import os
import pandas as pd
import azure.functions as func
import json
import logging
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from io import StringIO
from azure.keyvault.secrets import SecretClient

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

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    # Specify storage specifics
    account_name = 'dlscddatabreind1'
    account_key = get_secret('dls-databrein-d1-v2') # GET FROM KV?
    container_name = 'ga-csv-files'
    folder_name = 'GA_API'  # Specify your desired folder

    # Google Analytics credentials
    property_id = get_secret('GA-PropertyID')
    service_account_secret = get_secret('GA-JSON') # Moeten we deze inlezen als json file?

    # Set up the service account json
    service_account_json = json.loads(service_account_secret)  

    ga_credentials = service_account.Credentials.from_service_account_info(service_account_json)
    client = BetaAnalyticsDataClient(credentials = ga_credentials)
    logging.info('Client was set up with credentials')
    
    # function to store files as blobs in the dls
    def store_in_dls(account_name, account_key, folder_name, file_name, data):
      # Initialize BlobServiceClient with your Azure Storage account credentials
      blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net",
                                            credential=account_key)
    
      # Get a BlobClient for your Data Lake Storage folder
      blob_client = blob_service_client.get_blob_client(container=container_name,
                                                    blob=f"{folder_name}/{file_name}")
    
      blob_client.upload_blob(data, overwrite = True)


    # Runs a report of active users grouped by three dimensions.
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="date")],
        metrics=[
            Metric(name="activeUsers"),
            Metric(name="newUsers"),
            Metric(name="firstTimePurchaserRate"),
            Metric(name="firstTimePurchasersPerNewUser"),
            Metric(name="ecommercePurchases"),
            Metric(name="userEngagementDuration"),
            Metric(name="totalRevenue"),
        ],
        date_ranges=[DateRange(start_date="2023-06-01", end_date="yesterday")],
    )
    
    response = client.run_report(request)
    logging.info('Response with the requested dimensions and metrics was returned')
    
    dimension_values_list   = []
    metric_values_list      = []
    dimension_names = [dimension.name.split(': ')[0] for dimension in request.dimensions]
    metric_names    = [metric.name.split(': ')[0] for metric in request.metrics]
    
    
    for row in response.rows:
        dimension_values_list.append(row.dimension_values[0].value)
        # Extract metric values and store them as a list
        metric_values = [metric.value for metric in row.metric_values]
        metric_values_list.append(metric_values)
    
    data = {'Dimension': dimension_values_list}
    for i, metric_values in enumerate(zip(*metric_values_list)):
        data[f'Metric_{i}'] = metric_values
    
    dimension_names.extend(metric_names)
    
    df = pd.DataFrame(data)
    df.columns = dimension_names
    df['userEngagementDuration'] = df['userEngagementDuration'].astype(int)
    df['activeUsers'] = df['activeUsers'].astype(int)
    df['userEngagementDurationAverage'] = df['userEngagementDuration'] / df['activeUsers']
    logging.info('The dataframe was created')

    # Give the csv file a name
    csv_filename = 'GA_Webstore_Data.csv'
  
    # Setup IO string to prevent local storage
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index = False, sep = ';')
    csv_data = csv_buffer.getvalue()
    logging.info(f'{csv_filename} was converted to io object')
    
    store_in_dls(account_name = account_name, account_key = account_key, folder_name = folder_name, file_name = csv_filename, data = csv_data)
    logging.info(f'{csv_filename} was store in the dls')
    
    return func.HttpResponse(f'{csv_filename} was uploaded to the data lake storage')
