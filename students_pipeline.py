import gspread
import psycopg
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
gc = gspread.service_account()

#print(sh.sheet1.get('H1100'))
def get_env()->dict:
    return {
        "db_host" : os.getenv("db_host"),
        "db_name"  : os.getenv("db_name"),
        "db_user" : os.getenv("das_admin"),
        "db_password" : os.getenv("db_password"),
        "db_port" : os.getenv("db_port")
    }


# Get all values from a Google worksheet and ingest it into a pandas data frame
def get_all_ws_values(gc:gspread.service_account, wb_name: str, ws_name:str, header_row:int=1, data_row:int=2)->pd.DataFrame:
    wb = gc.open(wb_name)
    sh = wb.worksheet(ws_name)
    raw_data = sh.get_all_values()
    headers = raw_data[header_row]                   # Second row as headers
    rows = raw_data[data_row:]                     # Data starts from third row
    return pd.DataFrame(rows,columns=headers)

stocktakers = get_all_ws_values(gc,"Updated Dial A Stocktaker Application Form (Responses) NEW","Form responses 1")

for col in list(stocktakers.columns):
    print(col)
