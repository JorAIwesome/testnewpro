import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("De voorbeeldfunctie werkte", status_code=200)