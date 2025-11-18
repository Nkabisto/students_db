import gspread
import psycopg2
from psycopg2 import connect
import pandas as pd
from dotenv import load_dotenv
import os
import re
import logging
import io
from config import CANONICAL_COLUMNS, mapping_dict, combined_students_table_query, SHEET_CONFIGS 

# Log events of interst
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
def get_all_ws_values(gc:gspread.client.Client,applicant:dict ,header_row:int=1, data_row:int=2)->pd.DataFrame:
    try:
        wb_name = applicant["spreadsheet"]
        ws_name = applicant["worksheet"]
        unique_field = applicant["unique_field"]
        header_row = header_row if header_row is not None  else applicant.get("header_row",1)
        data_row = data_row if data_row is not None else applicant.get("data_row",2)

        wb = gc.open(wb_name)
        sh = wb.worksheet(ws_name)
        raw_data = sh.get_all_values()

        if len(raw_data) <= header_row:
            logger.warning(f"No header row found in {ws_name}")
            return pd.DataFrame()

        headers = raw_data[header_row]                   # Second row as headers
        rows = raw_data[data_row:] if len(raw_data) > data_row else []                     # Data starts from third row
        if not rows:
            logger.warning(f"No data rows found in {ws_name}")
            return pd.DataFrame()

        df = pd.DataFrame(rows,columns=headers)

        # Normalize column names here FIRST
        df = normalize_df_columns(df)
        logger.info(f"Normalised columns: {list(df.columns)[:16]}")

        # Convert timestamp column  if available
        timestamp_col = next((c for c in df.columns if "timestamp" == c), None)

        if timestamp_col:
            df[timestamp_col] = pd.to_datetime(df[timestamp_col], format='mixed', dayfirst=True,errors='coerce') # Convert Timestamp column to datetime
        else:
            logger.warning(f"No timestamp column found in worksheet {ws_name}")

        df = df.sort_values(by='timestamp') # Sort by timestamp

        return df.drop_duplicates(subset=[unique_field], keep='last') # Remove duplicates based on unqique_field and keep the last record 

    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"Worksheet {ws_name} not found in {wb_name}")
        return pd.DataFrame()
    except KeyError as e:
        logger.error(f"Missing configuration key: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading {ws_name}: {e}")
        return pd.DataFrame()

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

def validate_dataframe(df: pd.DataFrame)->bool:
    """Validate dataframe before database insertion"""
    if df.empty:
        logger.error("DataFrame is empty, cannot proceed")
        return False
    
    required_columns = ["id_number","first_names","surname"]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    # Check for null ID numbers
    if df["id_number"].isna().all():
        logger.error("All ID numbers are null")
        return False

    # Check for empty strings in ID numbers
    if df["id_number"].astype(str).str.strip().eq('').all():
        logger.error("All ID numbers are empty strings")
        return False
    
    logger.info("DataFrame validation passed")
    return True

if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    gc = gspread.service_account("./das-students-007f6500ea37.json")
    applications_df = []
    for applicant, config in SHEET_CONFIGS.items():
        logger.info(f"Getting values for {applicant} applications")
        df = get_all_ws_values(gc,config)
        if not df.empty:
            df = normalize_and_map(df)
            applications_df.append(df)
        else:
            logger.warning(f"No data retrieved for {applicant}")

    db_name = os.getenv("DB_NAME")
    db_host= os.getenv("DB_HOST")
    db_pwd= os.getenv("DB_PWD")
    db_port= os.getenv("DB_PORT")
    db_user= os.getenv("DB_USER")

    conn_string = f"dbname={db_name} user={db_user} password={db_pwd} host={db_host} port={db_port}"


    logger.info("Combining dataframes")
    combined_df = pd.DataFrame()
    for df in applications_df:
        if not df.empty:
            combined_df = combined_df.combine_first(df)
    
    if combined_df.empty:
        logger.error("No data available after combining all sources")
        exit(1)

    if not validate_dataframe(combined_df):
        logger.error("Data validation failed.Exiting.")
        exit(1)

    logger.info("Final combined dataframe")
    logger.info(combined_df.sample(30))

    try:
        with psycopg2.connect(conn_string) as con:
            logger.info("Getting registered stocktakers")
            with con.cursor() as cur:
                cur.execute("SELECT * FROM staging_stocktaker_tb")
                activelist_columns = [desc[0] for desc in cur.description]
                stkers = cur.fetchall()

                registered_stocktakers_df = pd.DataFrame(stkers, columns=activelist_columns)
                registered_stocktakers_df = normalize_and_map(registered_stocktakers_df)
                combined_df = combined_df.combine_first(registered_stocktakers_df)

                if not validate_dataframe(combined_df):
                    logger.error("Data validation failed after combining with registered stocktakers")
                    exit(1)

                # Create CSV buffer
                buffer = io.StringIO()
                combined_df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                table_name = "combined_students_table"
                temp_table = f"{table_name}_temp"
                createTableIfNotFound(con,table_name,combined_students_table_query)

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
        logger.info(f"Database connection failed: {e}")

