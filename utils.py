import azure.functions as func

def voorbeeld_functie(req: func.HttpRequest):
    return None  # Return None if param_name not found in both query parameters and JSON data
