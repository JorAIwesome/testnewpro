# VOEG AZ en SQL connecties toe

import requests
import logging
import os
import sys
import tempfile
import netCDF4 as nc
import pandas as pd
import time
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from azure.identity import ClientSecretCredential
from io import StringIO, BytesIO
import pyodbc
from azure.keyvault.secrets import SecretClient
import urllib


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
    
server = 'sql-cd-databrein-d1.database.windows.net'
database = get_secret("SQL-DATABASE")
username = get_secret("SQL-USERNAME")
password = get_secret("SQL-PASSWORD")
driver = 'ODBC Driver 17 for SQL Server'  # Is the driver installed?

connection_string = f'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(
    f'DRIVER={{{driver}}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password}'
)

engine = create_engine(connection_string)

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

latest_file = 'KIS___OPER_P___OBS_____L2.nc'
file_name = latest_file

class OpenDataAPI:
    def __init__(self, api_token: str):
        self.base_url = "https://api.dataplatform.knmi.nl/open-data/v1"
        self.headers = {"Authorization": api_token}

    def __get_data(self, url, params=None):
        return requests.get(url, headers=self.headers, params=params).json()

    def list_files(self, dataset_name: str, dataset_version: str, params: dict):
        return self.__get_data(
            f"{self.base_url}/datasets/{dataset_name}/versions/{dataset_version}/files",
            params=params,
        )

    def get_file_url(self, dataset_name: str, dataset_version: str, file_name: str):
        return self.__get_data(
            f"{self.base_url}/datasets/{dataset_name}/versions/{dataset_version}/files/{file_name}/url"
        )


def download_file_from_temporary_download_url(download_url, filename):
    try:
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            nc_file_buffer = BytesIO()
            for chunk in r.iter_content(chunk_size=8192):
                nc_file_buffer.write(chunk)
            nc_file_buffer.seek(0)  # Reset buffer position to the beginning
            return nc_file_buffer
    except Exception:
        logger.exception("Unable to download file using download URL")
        sys.exit(1)

    logger.info(f"Successfully downloaded dataset file to {filename}")


def main(req: func.HttpRequest) -> func.HttpResponse:

    # Azure Data Lake Storage settings
    account_name = 'dlscddatabreind1'
    account_key = get_secret('dls-databrein-d1-v2') # GET FROM KV?
    container_name = 'knmi-nc-files'
    folder_name = 'KNMI - Meteo data - daily'  # Specify your desired folder

    api_key = get_secret('KNMI-API-Key') # PUT THIS IN KV
    dataset_name = "etmaalgegevensKNMIstations" #/versions/1/files Dit staat in CLASS OpenDataAPI: def get_file_url
    dataset_version = "1"
    logger.info(f"Fetching latest file of {dataset_name} version {dataset_version}")

    api = OpenDataAPI(api_token=api_key)

    # sort the files in descending order and only retrieve the first file
    params = {"maxKeys": 1, "orderBy": "created", "sorting": "desc"}
    response = api.list_files(dataset_name, dataset_version, params)
    if "error" in response:
        logger.error(f"Unable to retrieve list of files: {response['error']}")
        sys.exit(1)

    print(response)
    latest_file = response["files"][0].get("filename")
    logger.info(f"Latest file is: {latest_file}")

    # fetch the download url and download the file
    response = api.get_file_url(dataset_name, dataset_version, latest_file)
    buffer = download_file_from_temporary_download_url(response["temporaryDownloadUrl"], latest_file)

    def store_in_dls(account_name, account_key, folder_name, file_name, data):
        # Initialize BlobServiceClient with your Azure Storage account credentials
        blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net",
                                                credential=account_key)

        # Get a BlobClient for your Data Lake Storage folder
        blob_client = blob_service_client.get_blob_client(container=container_name,
                                                        blob=f"{folder_name}/{file_name}")

        blob_client.upload_blob(data, overwrite = True)

        return print(f'{file_name} was uploaded to the data lake storage')
    
    store_in_dls(account_name = account_name, account_key = account_key, folder_name = folder_name, file_name = latest_file, data = buffer) 

    # Optionally, delete the local downloaded file
    #os.remove(latest_file)

    #  ZORG ERVOOR DAT DE FILE IN DE DLS TERECHTKOMT
    # ALLES ONDER MAIN ZETTEN?
    #    return f"File {latest_file} uploaded successfully to {container_name}/{folder_name}"

    #if __name__ == "__main__":
    #    main()


  # Function to flatten multi-dimensional arrays without adding dimension columns
    def flatten_array(arr, var_name):
        if arr.ndim == 0:  # Scalar value
            return pd.DataFrame({var_name: [arr.item()]})
        elif arr.ndim == 1:  # 1-dimensional array
            return pd.DataFrame(arr, columns=[var_name])
        else:  # Multi-dimensional array
            return pd.DataFrame(arr.reshape(-1), columns=[var_name])

    reference_date = datetime(1950, 1, 1, 0, 0, 0)
    def convert_time_to_date(num_time, time_units):
        # Define the reference date       
        # Convert time based on units
        if time_units.startswith('days since'):
            days = num_time
            return reference_date + timedelta(days= days - 1)
        elif time_units.startswith('seconds since'):
            seconds = num_time
            return reference_date + timedelta(seconds=seconds)
        else:
            raise ValueError("Unsupported time units. Supported units are 'days since' or 'seconds since'.")

    
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

            # Save the content to a temporary file
            logging.info("Writing content to a temporary file.")
            with tempfile.NamedTemporaryFile(delete=False, suffix=".nc") as temp_file:
                temp_file.write(file_content)
                temp_file_path = temp_file.name
                logging.info(f"Temporary file created at {temp_file_path}")

            return temp_file_path

        except Exception as e:
            logging.error(f"Failed to read the NetCDF file: {e}")
            return None
    
            

    def create_normalized_df(file):  
        try:
            # Verify that the file exists and is not empty
            logging.info(f"Opening NetCDF file: {file}")
            with open(file, 'rb') as f:
                content = f.read()
                if not content:
                    logging.error("NetCDF file is empty.")
                    return None

            # Open the dataset
            with nc.Dataset(file, 'r') as dataset:
                # Log the dataset details
                logging.info(f"NetCDF Dataset details: {dataset}")
                logging.info(f"Dataset dimensions: {dataset.dimensions}")
                logging.info(f"Dataset variables: {dataset.variables}")

                # Check if 'time' variable exists
                if 'time' not in dataset.variables:
                    logging.error("'time' variable not found in the NetCDF dataset.")
                    return None

                # Access the 'time' variable
                time_var = dataset.variables['time']
                logging.info(f"Time variable details: {time_var}")

                # Access the time values
                time_values = time_var[:]
                time_units = time_var.units

                # Extract the dimensions
                stations = dataset.variables['station'][:]
                days = [convert_time_to_date(time_val, time_units) for time_val in time_values]

                times = []
                for t in days:
                    if isinstance(t, float) and t >= 9.969209968386869e+35:
                        times.append(np.nan)  # Replace _FillValue with NaN or handle appropriately
                    else:
                        times.append(t) # t is already the reference time + presented time.

                excluded_vars = {'station', 'time', 'lat', 'lon', 'iso_dataset', 'product', 'projection'}
                variable_names = [var for var in dataset.variables.keys() if var not in excluded_vars]

                variables = {var_name: dataset.variables[var_name][:] for var_name in variable_names}
                long_names = {var_name: dataset.variables[var_name].long_name for var_name in variable_names}

                # Prepare the data for the DataFrame
                data = []
        
                #for i, station in enumerate(stations):
                for j, time in enumerate(times):
                    row = {
                        'Station': '260',
                        'Time': time,
                    }
                    for var_name in variable_names:
                        row[long_names[var_name]] = variables[var_name][18, j] # index 18 is the bilt
                    data.append(row)

                # Create the DataFrame
                df = pd.DataFrame(data)
                df['Time'] = df['Time'] - pd.Timedelta(days=1)

            return df
        
        except Exception as e:
            logging.error(f"Failed to process the NetCDF file: {e}")
            return None
    
    
    file_content = access_file_from_adls(account_name, account_key, container_name, folder_name, latest_file)
    #if file_content is not None:
    #    logger.info(f"{file_name} was accessed and read")
    #    logger.info(f"The type is {type(file_content)}")
    #    logger.info(f"The path is {file_content }")
    #else:
    #    logger.error(f"Failed to access or read {file_name}")
    
    
    if file_content:
        logger.info(f"{latest_file} was accessed and downloaded to {file_content}")
        df = create_normalized_df(file_content)
        if df is not None:
            logger.info("DataFrame created successfully.")
            logger.info(df.head())
        else:
            logger.error("Failed to create DataFrame.")
        # Clean up the temporary file after processing
        os.remove(file_content)
    else:
        logger.error(f"Failed to access or download {latest_file}")

    
    csv_filename = 'KNMI_Data_Daily.csv'

    # Setup IO string to prevent local storage
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index = False, sep = ';')
    csv_data = csv_buffer.getvalue()
    
    store_in_dls(account_name = account_name, account_key = account_key, folder_name = folder_name, file_name = csv_filename, data = csv_data)
    
    # Sla de gegevens op in de database
    
    #logging.info(database)
    #logging.info(username)
    #affected_rows = filtered_df.to_sql("KNMI-MeteoDaily", engine, if_exists='append', index=False)
        
    #logger.info(f'Upload gedaan')
    # engine.dispose()
  
    return func.HttpResponse(f"Files {latest_file} and {csv_filename} were uploaded successfully to {container_name}/{folder_name}")
