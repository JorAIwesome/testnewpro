import os
import pyodbc
import os
# Retrieve environment mode
env_mode = os.getenv('ENV_MODE')

if env_mode != 'production':
    conn_string = 'DRIVER={SQL Server};SERVER=localhost,1433;DATABASE=sql-ds-bi-p1;UID=sa;PWD=%6VqbnE8QPdD&kwDHHw'
else:
    from get_secret import get_secret

    database = get_secret("SQL_DATABASE")
    username = get_secret("SQL_USERNAME")
    password = get_secret("SQL_PASSWORD")
    conn_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:sql-ds-bi-p1.database.windows.net,1433;DATABASE="+database+";UID="+username+";PWD="+password+";"

conn = pyodbc.connect(conn_string)
cursor = conn.cursor()