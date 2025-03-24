import gspread
from google.oauth2.service_account import Credentials
from pyspark.sql import SparkSession

# Define the scope required for the Google Sheets API
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Path to the service account credentials JSON file
credenciales_json = './.ignore/google_sheets.json'

# Authenticate using the service account credentials
creds = Credentials.from_service_account_file(credenciales_json, scopes=scope)
client = gspread.authorize(creds)  # Authorize the client using the credentials

# Open the Google Sheets document by its name
spreadsheet = client.open('pyspark')  # Replace 'pyspark' with the name of your sheet
sheet = spreadsheet.sheet1  # Access the first sheet of the spreadsheet

# Clear the existing content in the sheet
sheet.clear()  # This will remove all existing data in the sheet

# Create a PySpark session
spark = SparkSession.builder.appName("CSV_to_GoogleSheets").getOrCreate()

# Read a CSV file into a PySpark DataFrame
df = spark.read.csv("./data/cleaned_data_output/cleaned_data.csv", header=True, inferSchema=True)

# Convert the PySpark DataFrame into a list of rows
rows = df.collect()  # Collect the rows into a list
data = [list(row) for row in rows]  # Convert each row into a list

# Insert the DataFrame column headers as the first row in the sheet
sheet.insert_row(df.columns, 1)  # Insert column headers in the first row

# Append the rows from the DataFrame into the sheet
sheet.append_rows(data)  # Append all rows from the data list to the sheet

print("Data written to Google Sheets successfully.")  # Print a success message