import gspread
import psycopg2
from psycopg2 import connect
import pandas as pd
from dotenv import load_dotenv
import os
import re


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

#_______________________________canonical schema -----------------------------------------
CANONICAL_COLUMNS = [
    "timestamp","first_names","surname","id_number","contact_number","alternate_contact_number","email","street_address","suburb","city",
    "province","postal_code","sars_number","beneficiary_number","banking_institution","bank_account_number","account_type"]

mapping_dict={
    "timestamp":"timestamp",
    "last_updated":"timestamp",
    "surname":"surname",
    "last_name":"surname",
    "first_names": "first_names",
    "first_name":"first_names",
    "id_number": "id_number",
    "id" : "id_number",
    "identity_number":"id_number",
    "south_african_id_number":"id_number",
    "street_address":"street_address",
    "street": "street_address",
    "residential_street_address":"street_address",
    "permanent_home_address":"street_address",
    "suburb":"suburb",
    "suburb_township":"suburb",
    "residential_suburb":"suburb",
    "citytown":"city",
    "city_town":"city",
    "residential_citytown":"city",
    "code":"postal_code",
    "post_code":"postal_code",
    "postal_code":"postal_code",
    "province":"province",
    "contact_number":"contact_number",
    "cellphone":"contact_number",
    "cellular_numbers":"contact_number",
    "whatsapp_number":"alternate_contact_number",
    "secondary_contact_number": "alternate_contact_number",
    "telephone":"alternate_contact_number",
    "sars_tax_number":"sars_tax_number",
    "sars_number":"sars_tax_number",
    "sars_number_if_you_have_one": "sars_tax_number",
    "banking_institution":"banking_institution",
    "bank_account_number":"bank_account_number",
    "account_type":"account_type",
    "bank_account_type":"account_type"

}

# ------------------CREATE TABLE IF NOT EXISTS statement baseed on your combined_df.dtypes, assuming column names and types match the canonical schema
combined_students_table_query = """
CREATE TABLE IF NOT EXISTS combined_students_table (
    timestamp TIMESTAMP,
    first_names TEXT,
    surname TEXT,
    id_number TEXT,
    contact_number TEXT,
    alternate_contact_number TEXT,
    email TEXT,
    street_address TEXT,
    suburb TEXT,
    city TEXT,
    province TEXT,
    postal_code TEXT,
    sars_number TEXT,
    beneficiary_number TEXT,
    banking_institution TEXT,
    bank_account_number TEXT,
    account_type TEXT
);
"""
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

def createTableIfNotFound(con: connect, table_name: str, schema: str):
    try:
        # Check if table exists, if not, create it
        with con.cursor() as cur:
            cur.execute("""
                SELECT EXISTS(
                    SELECT FROM information_schema.tables
                    WHERE table_name = %s
                );
            """, (table_name,))
            table_exists = cur.fetchone()[0]

            if not table_exists:
                logger.info(f"Table {table_name} does not exist. Creating it.")
                with con.cursor() as cur:
                    cur.execute(schema)
                con.commit()

    except psycopg2.Error as e:
        logger.error(f"Error checking/creating table {table_name}: {e}")
        con.rollback()   

if __name__ == "__main__":
    stocktaker_app_spreadsheet= "Updated Dial A Stocktaker Application Form (Responses) NEW"
    stocktaker_app_response = "Form responses 1"

    dial_a_student_spreadsheet = "Dial a Student Application Form (2nd Version) (Responses)"
    dial_a_student_response = "Form responses 1"

    coordinators_app_spreadsheet ="Co-ordinator Online Application Form (Responses) OUR Version" 
    coordinators_app_response ="Form Responses 1" 

    back_area_app_spreadsheet ="Back Area Online Application Form (Responses)"
    back_area_app_response = "Form Responses 1"


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

    registered_stocktakers_df = normalize_and_map(registered_stocktakers_df)

    print("Combining dataframes")
    combined_df = stocktakers_df.combine_first(das_students_df)
    combined_df = combined_df.combine_first(back_area_df)
    combined_df = combined_df.combine_first(coordinators_df)
    combined_df = combined_df.combine_first(registered_stocktakers_df)

    print("Final combined dataframe")
    print(combined_df.sample(30))

    try:
        with psycopg2.connect(conn_string) as con:
            print("Getting registered stocktakers")
            with con.cursor() as cur:
                cur.execute("SELECT * FROM staging_stocktaker_tb")
                activelist_columns = [desc[0] for desc in cur.description]
                stkers = cur.fetchall()
                registered_stocktakers_df = pd.DataFrame(stkers, columns=activelist_columns)

                # Create CSV buffer
                buffer = io.StringIO()
                combined_df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                table_name = "combined_students_table"
                temp_table = f"{table_name}_temp"

                conflict_keys = ["id_number"]
                selected_columns = list(combined_df.columns)
                update_clause = ", ".join(
                    f"{col} = EXCLUDED.{col}" for col in selected_columns if col not in conflict_keys
                )
                cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
                cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {table_name} INCLUDING ALL)")
                # Bulk insert into temp table
                cur.copy_expert(f"COPY {temp_table} FROM STDIN WITH CSV", buffer)

                upsert_sql = f"""
                INSERT INTO {table_name} ({', '.join(selected_columns)})
                SELECT {', '.join(selected_columns)} FROM {temp_table}
                ON CONFLICT ({', '.join(conflict_keys)}) DO UPDATE SET {update_clause};
                """
                cur.execute(upsert_sql)
                con.commit()

    except Exception as e:
        print(f"Database connection failed: {e}")

