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

def normalize_column_name(name:str)->str:
    s = str(name).lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "",s)
    s = re.sub(r"_+", "_",s)
    s = re.sub("_")
    return s or str(name)

def normalize_df_columns(df: pdf.DataFrame)->pd.DataFrame:
    mapping = {col: normalize_column_name(col) for col in df.columns}
    return df.rename(columns=mapping)

def coerce_str(x):
    if pd.isna(x):
        return None
    return str(x).strip()

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

#_______________________________canonical schema -----------------------------------------
CANONICAL_COLUMNS = [
    "timestamp","surname","first_names","id_number","email","street_address","suburb","city","postal_code",
    "province","contact_number","alternate_contact_number","province","sars_number","beneficiary_number",
    "banking_institution","bank_account_number","account_type"
]

mapping_df={
    "timestamp":"timestamp",
    "surname":"surname",
    "last_name":"surname",
    "first_names": "first_names",
    "id_number": "id_number",
    "identity_number":"id_number",
    "south_african_id_number":"id_number",
    "street_address":"street_address",
    "residential_street_address":"street_address",
    "permanent_home_address":"street_address",
    "suburb":"suburb",
    "residential_suburb":"suburb",
    "citytown":"city",
    "residential_citytown":"city",
    "code":"postal_code",
    "post_code":"postal_code",
    "postal_code":"postal_code",
    "province":"province",
    "contact_number":"contact_number",
    "cellphone":"contact_number",
    "cellular_numbers":"contact_number",
    "whatsapp_number":"alternate_contact_number",



    "city





}
stocktaker_app_spreadsheet= "Updated Dial A Stocktaker Application Form (Responses) NEW"
stoctkater_app_response = "Form responses 1"

dial_a_student_spreadsheet = "Dial a Student Application Form (2nd Version) (Responses)"
dial_a_student_response = "Form responses 1"

coordinators_app_spreadsheet ="Co-ordinator Online Application Form (Responses) OUR Version" 
coordinators_app_response ="Form Responses 1" 

back_area_app_spreadsheet ="Back Area Online Application Form (Responses)"
back_area_app_response = "Form Responses 1"

print("Getting values for Stocktaker applications")
stocktakers_df = get_all_ws_values(gc,stocktaker_app_spreadsheet,stoctkater_app_response,"ID Number")
print("Getting values for Dial A Students applications")
das_students_df = get_all_ws_values(gc,dial_a_student_spreadsheet,dial_a_student_response,"South African ID Number")
print("Getting values for Coordinator applications")
coordinators_df = get_all_ws_values(gc,coordinators_app_spreadsheet,coordinators_app_response,"Identity Number :") 
print("Getting values for Back Area applications")
back_area_df = get_all_ws_values(gc,back_area_app_spreadsheet ,back_area_app_response,"Identity Number :",0,1)

print(coordinators_df)
print(back_area_df)

'''for col in list(das_students_df.columns):
    print(col)
'''

