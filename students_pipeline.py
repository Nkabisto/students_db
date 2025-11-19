import gspread
import psycopg2
from psycopg2 import connect
import pandas as pd
from dotenv import load_dotenv
import os
import re
from config import CANONICAL_COLUMNS, mapping_dict, SHEET_CONFIGS

# Load environment variables from .env file
load_dotenv()
gc = gspread.service_account("./das-students-007f6500ea37.json")

def normalize_column_name(name:str)->str:
    s = str(name).lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "",s)
    s = re.sub(r"_+", "_",s)
    s = s.strip("_")
    return s or str(name)

def normalize_df_columns(df: pd.DataFrame)->pd.DataFrame:
    mapping = {col: normalize_column_name(col) for col in df.columns}
    return df.rename(columns=mapping)

def normalize_str(x)->str | None:
    if pd.isna(x):
        return None
    s = str(x).strip()
    return s.title() if s != "" else None

def normalize_phone(x:str)->str | None:
    if pd.isna(x) or str(x).strip() =="":
        return None
    s = re.sub(r"[^\d+]", "", str(x)) # keep digits and + if present
    return s or None

def normalize_number(x:str)->str | None:
    if pd.isna(x):
        return None
    s = re.sub(r"\s+", "", str(x))
    return s or None

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

# -----------------------------------normalization & mapping function ---------------------------------------
def normalize_and_map(df:pd.DataFrame, mapping:dict[str,str]=mapping_dict, canonical_cols:list[str]=CANONICAL_COLUMNS)->pd.DataFrame:

    # normalize headers
    df = normalize_df_columns(df)

    # Rename columns to those in the mapping
    df_mapped = df.rename(columns=mapping)

    # 🔥 Drop duplicate columns 
    df_mapped = df_mapped.loc[:, ~df_mapped.columns.duplicated(keep='first')].copy()


    # Insert canonical columns not found in the dataframe and initialize to zero
    for col in canonical_cols:
        if col not in df_mapped.columns:
            df_mapped[col] = None

    # Restructure dataframe to only include mapped columns (in desired order)
    df_final = df_mapped[canonical_cols].copy()

    #Normalize specific columns 
    for col in df_final.columns:
        if col == "timestamp":# do nothing if its time stamp
            continue 
        elif col in {"contact_number","alternate_contact_number"}:
            df_final[col] = df_final[col].apply(normalize_phone) # Remove spaces between digits and plus sign (if present)
        elif col in {"id_number","postal_code","sars_number","beneficiary_number"}:
            df_final[col] = df_final[col].apply(normalize_number) # Remove spaces between digits
        else:
            df_final[col] = df_final[col].apply(normalize_str)# For the rest of the columns simply clean the strings
    
    return df_final
    
    
stocktaker_app_spreadsheet= "Updated Dial A Stocktaker Application Form (Responses) NEW"
stocktaker_app_response = "Form responses 1"

dial_a_student_spreadsheet = "Dial a Student Application Form (2nd Version) (Responses)"
dial_a_student_response = "Form responses 1"

coordinators_app_spreadsheet ="Co-ordinator Online Application Form (Responses) OUR Version" 
coordinators_app_response ="Form Responses 1" 

back_area_app_spreadsheet ="Back Area Online Application Form (Responses)"
back_area_app_response = "Form Responses 1"

def extract_gs_and_normalize():
print("Getting values for Stocktaker applications")
stocktakers_df = get_all_ws_values(gc,stocktaker_app_spreadsheet,stocktaker_app_response,"ID Number")
stocktakers_df = normalize_and_map(stocktakers_df)
print("Getting values for Dial A Students applications")
das_students_df = get_all_ws_values(gc,dial_a_student_spreadsheet,dial_a_student_response,"South African ID Number")
das_students_df = normalize_and_map(das_students_df )
print("Getting values for Coordinator applications")
coordinators_df = get_all_ws_values(gc,coordinators_app_spreadsheet,coordinators_app_response,"Identity Number :") 
coordinators_df = normalize_and_map(coordinators_df )
print("Getting values for Back Area applications")
back_area_df = get_all_ws_values(gc,back_area_app_spreadsheet ,back_area_app_response,"Identity Number :",0,1)
back_area_df = normalize_and_map(back_area_df )

db_name = os.getenv("DB_NAME")
db_host= os.getenv("DB_HOST")
db_pwd= os.getenv("DB_PWD")
db_port= os.getenv("DB_PORT")
db_user= os.getenv("DB_USER")

conn_string = f"dbname={db_name} user={db_user} password={db_pwd} host={db_host} port={db_port}"

registered_stocktakers_df = pd.DataFrame()

try:
    with psycopg2.connect(conn_string) as con:
        print("Getting values from the Activelist")
        with con.cursor() as cur:
            cur.execute("SELECT * FROM staging_stocktaker_tb")
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()
            registered_stocktakers_df = pd.DataFrame(data, columns=columns)

except Exception as e:
    print(f"Database connection failed: {e}")
#    print("Make sure PostgreSQL is running: sudo systemctl start postgresql")

registered_stocktakers_df = normalize_and_map(registered_stocktakers_df)

print("Combining dataframes")
combined_df = stocktakers_df.combine_first(das_students_df)
combined_df = combined_df.combine_first(back_area_df)
combined_df = combined_df.combine_first(coordinators_df)
combined_df = combined_df.combine_first(registered_stocktakers_df)

print("Final combined dataframe")
print(combined_df)
combined_df.to_csv("combined_students.csv", index=False)
