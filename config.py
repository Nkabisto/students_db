
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
    "sars_tax_number":"sars_number",
    "sars_number":"sars_number",
    "sars_number_if_you_have_one": "sars_number",
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
SHEET_CONFIGS ={
    "stocktaker":{
        "spreadsheet": "Updated Dial A Stocktaker Application Form (Responses) NEW",
        "worksheet": "Form responses 1",
        "unique_field": "id_number",#"ID Number",
        "header_row":1,
        "data_row": 2
    },
    "student":{
        "spreadsheet":"Dial a Student Application Form (2nd Version) (Responses)",
        "worksheet":"Form responses 1",
        "unique_field": "south_african_id_number", #"South African ID Number",
        "header_row":1,
        "data_row": 2
    },
    "coordinator":{
        "spreadsheet": "Co-ordinator Online Application Form (Responses) OUR Version",
        "worksheet":"Form Responses 1",
        "unique_field": "identity_number",#"Identity Number :",
        "header_row":1,
        "data_row": 2
    },
    "back_area":{
        "spreadsheet":"Back Area Online Application Form (Responses)",
        "worksheet":"Form Responses 1",
        "unique_field": "identity_number",#"Identity Number :",
        "header_row":0,
        "data_row":1
    }
}


