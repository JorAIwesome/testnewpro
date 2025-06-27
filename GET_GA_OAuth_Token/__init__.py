import os
import json
import logging
from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from google.oauth2 import service_account
from google.auth.transport.requests import Request
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Get Key Vault URL from environment variables
        key_vault_url = os.getenv("KEY_VAULT_URL", "https://kv-cd-databrein-d1.vault.azure.net/")
        
        # Authenticate to Azure Key Vault
        credential = ManagedIdentityCredential()
        secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
        
        # Get the Google Service Account Key JSON from Key Vault
        secret_name = "GA-json"
        key_json = secret_client.get_secret(secret_name).value
        
        # Load the service account key
        service_account_info = json.loads(key_json)
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/analytics.readonly"]
        )
        
        # Obtain the access token
        credentials.refresh(Request())
        token = credentials.token

        return func.HttpResponse(
            json.dumps({"token": token}),
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
