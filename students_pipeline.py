import gspread
import psycopg
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
gc = gspread.service_account("./das-students-007f6500ea37.json")

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
def get_all_ws_values(gc:gspread.service_account, wb_name: str, ws_name:str,unique_field=str,header_row:int=1, data_row:int=2)->pd.DataFrame:
    wb = gc.open(wb_name)
    sh = wb.worksheet(ws_name)
    raw_data = sh.get_all_values()
    headers = raw_data[header_row]                   # Second row as headers
    rows = raw_data[data_row:]                     # Data starts from third row
    df = pd.DataFrame(rows,columns=headers)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='mixed', dayfirst=True,errors='coerce') # Convert Timestamp column to datetime
    df = df.sort_values(by='Timestamp') # Sort by Timestamp
    return df.drop_duplicates(subset=[unique_field], keep='last') # Remove duplicates based on unqique_field and keep the last record 

stocktaker_app_spreadsheet= "Updated Dial A Stocktaker Application Form (Responses) NEW"
stoctkater_app_response = "Form responses 1"

dial_a_student_spreadsheet = "Dial a Student Application Form (2nd Version) (Responses)"
dial_a_student_response = "Form responses 1"

coordinators_app_spreadsheet = 
coordinators_app_response

back_area_app_spreadsheet =
back_area_app_response = 

stocktakers_df = get_all_ws_values(gc,stocktaker_app_spreadsheet,stoctkater_app_response,"ID Number")
das_students_df = get_all_ws_values(gc,dial_a_student_spreadsheet,dial_a_student_response,"South African ID Number")
#print(das_students_df)

for col in list(das_students_df.columns):
    print(col)
