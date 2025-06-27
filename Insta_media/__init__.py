import requests
import logging
import os
import pandas as pd
import azure.functions as func
import datetime
from azure.identity import ClientSecretCredential
from azure.mgmt.storage import StorageManagementClient 
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from io import StringIO
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
folder_name = 'Insta_media'  # Specify your desired folder
# current_date = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d') # Use regular name since posts are not retrieved by date
csv_filename = 'Insta_posts.csv'

# Get the Meta llat
page_access_token = get_secret('Meta-Page-Token')
instagram_business_account_id = os.getenv('IG_BUSINESS_ACCOUNT_ID')

url = f'https://graph.facebook.com/v16.0/{instagram_business_account_id}/media?fields=id&access_token={page_access_token}'

def main(req: func.HttpRequest) -> func.HttpResponse:
  
  def process_media_data(data, cols):
      if 'data' in data:
          rows = [item for item in data['data']]
          #print(rows)
          return pd.DataFrame(rows, columns = cols)
      else:
          return pd.DataFrame([data], columns = cols)
  
  def paging_calls_media(url, df, cols):
      while url:
          response = requests.get(url, params={})
          if response.status_code != 200:
              raise Exception(f"Request failed: {response.text}")
          
          data = response.json()
          
          # Process the current page data
          df = pd.concat([df, process_media_data(data, cols)], ignore_index=True)
          
          # Check if there's a 'next' link in the pagination
          if 'paging' in data and 'next' in data['paging']:
              url = data['paging']['next']
          else:
              url = None
      return df
  
  # Make the request to get media data
  media_response = requests.get(url)
  
  # Check if the request was successful
  print(f'Status Code: {media_response.status_code}')
  
  # Print the response text to debug
  print(f'Response Text: {media_response.text}')
  
  # Initialize an empty DataFrame
  df_media_ids = pd.DataFrame()
  
  # Fetch and process paginated data
  cols = ['id']
  df_media_ids = paging_calls_media(url, df_media_ids, cols)
  
  fields = ['timestamp', 'id', 'caption', 'comments_count', 'like_count', 'media_product_type', 'media_type']
  fields_join = ",".join(fields)
  df_post_info = pd.DataFrame(columns = fields)
  
  for i, media_id in enumerate(df_media_ids['id']):
      post_id = df_media_ids.iloc[i,0]
      post_url = f'https://graph.facebook.com/v19.0/{post_id}?fields={fields_join}&access_token={page_access_token}'
      #print(post_url)
      
      try:
          post_response = requests.get(post_url)
          post_data = post_response.json()
          df = process_media_data(post_data, fields)
          df_post_info = pd.concat([df_post_info, df], axis = 0, ignore_index=True)
          
      except requests.exceptions.RequestException as e:
          print(f"Request failed for post ID {post_id}: {e}")
  
  # Setup IO string to prevent local storage
  csv_buffer = StringIO()
  df_post_info.to_csv(csv_buffer, index = False, sep = ';')
  csv_data = csv_buffer.getvalue()
  logging.info('Set up the csv buffer')

  ## Store the csv in the dls
  # Initialize BlobServiceClient with your Azure Storage account credentials
  blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net",
                                          credential=account_key)
  logging.info('Initialized BlobServiceClient')

  # Get a BlobClient for your Data Lake Storage folder
  blob_client = blob_service_client.get_blob_client(container=container_name,
                                                    blob=f"{folder_name}/{csv_filename}")

  blob_client.upload_blob(csv_data, overwrite = True)

  return func.HttpResponse(f'{csv_filename} was uploaded to {account_name}/{container_name}/{folder_name}')
