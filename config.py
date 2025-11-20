
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

SHEET_CONFIGS =[
    {
        "spreadsheet":"Updated Dial A Stocktaker Application Form (Responses) NEW",
        "tab":"Form responses 1",
        "unique_field":"ID Number",
        "header_row":1,
        "data_row":2
    },
    {
        "spreadsheet":"Dial a Student Application Form (2nd Version) (Responses)",
        "tab":"Form responses 1",
        "unique_field":"South African ID Number",
        "header_row":1,
        "data_row":2
    },
    {
        "spreadsheet":"Co-ordinator Online Application Form (Responses) OUR Version",
        "tab":"Form Responses 1",
        "unique_field":"Identity Number :",
        "header_row":1,
        "data_row":2
    },
    {
        "spreadsheet":"Back Area Online Application Form (Responses)",
        "tab":"Form Responses 1",
        "unique_field":"Identity Number :,
        "header_row":0,
        "data_row":1
    }
]
