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


def main(req: func.HttpRequest) -> func.HttpResponse:

    since_str = req.params.get('since')

    if not since_str:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            since_str = req_body.get('since')

    if since_str:
        try:
            # Parse the since_str to a datetime object
            since_req = datetime.datetime.strptime(since_str, '%Y-%m-%d') # Is deze methode correct?
        except ValueError:
            return func.HttpResponse(
                "Invalid date format. Please use YYYY-MM-DD.",
                status_code=400
            )
    else:
        return func.HttpResponse(
            "Please pass a date in the query string or in the request body",
            status_code=400
        )

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
    
    # Get the Meta llat
    page_access_token = get_secret('Meta-Page-Token')
    instagram_business_account_id = os.getenv('IG_BUSINESS_ACCOUNT_ID')
    logging.info(f'Length of the page_access_token object is {len(page_access_token)}')
    logging.info(f'Length of the ig_business_account_id object is {len(instagram_business_account_id)}')
    
    url = f'https://graph.facebook.com/v20.0/{instagram_business_account_id}/insights'
    
    # year = 2024 # Replaced by 'since' request input parameter
    # month = 6
    # day = 1

    def fetch_insights(url, params):
        response = requests.get(url, params=params)
        if response.status_code == 200:
            logging.info('Data was fetched with status code 200')
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            print(response.json())
            logging.info('Something went wrong with accessing the API')
            return None

    # Function to process the data and add it to the DataFrame
    def process_data(data, metric, df):
        if 'data' in data:
            for entry in data['data']:
                if entry['name'] == metric:
                    for value in entry['values']:
                        date = value['end_time']
                        # If the date is not already in the DataFrame, add it
                        if date not in df.index:
                            df.loc[date] = [None] * len(df.columns)
                        df.at[date, metric] = value['value']             
        return df
  
  # Function to process the total_value metrics
    def process_tv_data(data, params, metric):
        # use try to deal with empty values
        
        date = params['until']
        
        try:
            if params['breakdown'] == 'follow_type':
                data = data['data'][0]['total_value']['breakdowns'][0]['results']
                
                # Initialize a dictionary to store the values
                data_dict = {'FOLLOWER': 0, 'NON_FOLLOWER': 0}
    
                # Loop through the data and populate the dictionary
                for item in data:
                    dimension_value = item['dimension_values'][0]
                    value = item['value']
                    data_dict[dimension_value] = value
                    #print(data_dict)
                # Create a DataFrame with the date as the index
                df = pd.DataFrame([data_dict], index=[date])
                
            elif params['breakdown'] == '':
                data = data['data'][0]['total_value']['value']
                #print(f'data is {data}')
                df = pd.DataFrame([data], columns = [metric], index = [date])
            
        except (IndexError, KeyError) as e:
            print(f"Data was empty. No results: {e}")
            df = pd.DataFrame()
            return df
            
        return df
  
    def paging_calls(url, params, metric, df, type):
        # Get initial data
        data = fetch_insights(url, params)
        print(data)
        last_it = None
        
        if data:
          # Process the initial data
          #if not any(entry['name'] == metric for entry in data.get('data', [])):
          #    print(f"Metric {metric} not defined in the response.")
          #    return
            if data and type != 'total_value':
                df = process_data(data, metric, df)
            elif type == 'total_value':
                df = process_tv_data(data, params, metric)
            else:
                return print('INVALID TYPE WAS PROVIDED')
            
            # Check if there's a 'previous' link in the pagination
            while 'paging' in data and ('previous' in data['paging'] or 'next' in data['paging']):
                if "since" in params and last_it is None:            
    
                    if 'next' not in data['paging']:
                        last_it = 1
                        print('Reached current date')
                        break
                    
                    page_url = data['paging']['next']
                    print(f"Fetching next data from: {page_url}")
                    
                else:
                    page_url =  data['paging']['previous']
                    print(f"Fetching previous data from: {page_url}")
                
                # Make a request to the previous URL
                data = fetch_insights(page_url, {})
                
                if data and type != 'total_value':
                    df = process_data(data, metric, df)
                elif type == 'total_value':
                    params['until'] = params['until'] + datetime.timedelta(days = 1)
                    df_process = process_tv_data(data, params, metric)
                    df = pd.concat([df, df_process], axis = 0)
                else:
                    print("Failed to fetch previous data.")
                    break
            print('broken')
        return df

  

  ### Start applying the function to actually retrieve and process data

  ## Start by getting the regular value metrics
  # Initialize an empty DataFrame with date as index
    metrics = ['follower_count', 'reach', 'impressions']
    df = pd.DataFrame(columns=metrics)
    df.index.name = 'date'
  
  # Loop through each metric and get the data
    for metric in metrics:
      # Construct the URL
      url = f'https://graph.facebook.com/v20.0/{instagram_business_account_id}/insights'
      
      if metric == 'follower_count': # should be some except or try command? # in [special list]:?
          
          params = {
          'metric': metric,
          'access_token': page_access_token,
          'period': 'day'
          }
      
          df_paging = paging_calls(url, params, metric, df, type = 'values')
          df = pd.concat([df, df_paging], axis = 0)
      
      else:
          since = since_req
          until = since + datetime.timedelta(days = 30)
          
          params = {
          'metric': metric,
          'access_token': page_access_token,
          'period': 'day',
          'since': since,
          'until': until
          }
          
          df_paging = paging_calls(url, params, metric, df, type = 'values')
          df = pd.concat([df, df_paging], axis = 0)
          continue               
    logging.info('Looped through value metrics')
  
    # Sort the DataFrame by date
    df.index = pd.to_datetime(df.index)
    df.drop_duplicates(inplace=True)
    df.sort_index(inplace=True)   

    ## Get the total_value metrics
    total_value_metrics = ['follows_and_unfollows', 'accounts_engaged', 'profile_views', 'website_clicks']
    breakdown_metrics = ['follows_and_unfollows']
  
    df_tv = pd.DataFrame() # columns=total_value_metrics
    df_tv.index.name = 'date'
  
    for metric in total_value_metrics:
        since = since_req
        until = since + datetime.timedelta(days = 0)
            
        if metric in breakdown_metrics:
            breakdown = 'follow_type'
        else:
            breakdown = ''
                
        params = {
        'metric': metric,
        'access_token': page_access_token,
        'period': 'day',
        'since': since,
        'until': until,
        'breakdown': breakdown,
        'metric_type': 'total_value' 
        }
        
        # Ensure indexes are unique before concatenation
        if not df.index.is_unique:
            df = df.reset_index(drop=True)
    
        if not df_paging.index.is_unique:
            df_paging = df_paging.reset_index(drop=True)
        
        # Align indexes if necessary
        df_paging = df_paging.reindex(df.index)
        
        df_paging = paging_calls(url, params, metric, df, type= 'total_value')
        df_paging.index.name = 'date'
        df_tv = pd.concat([df_tv, df_paging], axis = 1)
    logging.info('Looped through total_value metrics')

    df.index = df.index.date
    df_tv.index = df_tv.index.date
    df_insights = pd.merge(left = df, right = df_tv, left_index = True, right_index = True, how = 'right') # Used to be left, but right is convenient for incremental loads since df_tv is subjected properly to the since_req paramter
    logging.info('merged df and df_tv')

    # Remove empty rows from df_insights
    columns_to_check = [5, 6, 7]
    # Remove rows with empty values in specified columns
    df_insights = df_insights.dropna(subset=df_insights.columns[columns_to_check])

    # Setup IO string to prevent local storage
    csv_buffer = StringIO()
    df_insights.to_csv(csv_buffer, index = True, index_label = 'date', sep = ';')
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
    logging.info('file was uploaded')

    return func.HttpResponse(f'{csv_filename} was uploaded to {account_name}/{container_name}/{folder_name}')
