import gspread
import psycopg
import pandas as pd
from dotenv import load_dotenv
import os


gc = gspread.service_account()

sh = gc.open("Updated Dial A Stocktaker Application Form (Responses) NEW")

print(sh.sheet1.get('H1100'))




