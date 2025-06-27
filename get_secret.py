# from azure.keyvault.secrets import SecretClient
# from azure.identity import DefaultAzureCredential

# credential = DefaultAzureCredential()
# client = SecretClient(vault_url="https://kv-ds-bi-p1.vault.azure.net/", credential=credential)
import os

def get_secret(secretName):
    return os.getenv(secretName)
