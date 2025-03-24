import gspread
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from pyspark.sql import SparkSession

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

credenciales_json = './.ignore/google_sheets.json'

creds = Credentials.from_service_account_file(credenciales_json, scopes=scope)
client = gspread.authorize(creds)

spreadsheet = client.open('pyspark')
sheet = spreadsheet.sheet1

sheet.clear()

spark = SparkSession.builder.appName("CSV_to_GoogleSheets").getOrCreate()

df = spark.read.csv("./data/cleaned_data_output/cleaned_data.csv", header=True, inferSchema=True)

rows = df.collect()
data = [list(row) for row in rows]

sheet.insert_row(df.columns, 1)

sheet.append_rows(data)

print("Data written to Google Sheets successfully.")