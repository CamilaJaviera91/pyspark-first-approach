import gspread
from google.oauth2.service_account import Credentials
from pyspark.sql import SparkSession

def write_to_gsheets(client, sheet_name, csv_path, spark):
    """Reads a CSV using PySpark and writes it to Google Sheets."""
    
    # Open the Google Sheets document by the given sheet name
    spreadsheet = client.open(sheet_name)
    sheet = spreadsheet.sheet1  # Access the first sheet

    # Clear the existing content in the sheet
    sheet.clear()

    # Read the CSV file into a PySpark DataFrame
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

    # Remove empty rows from the DataFrame
    df = df.na.drop(how="all")

    # Convert the DataFrame into a list of rows for Google Sheets
    rows = df.collect()  # Collect the rows into a list
    data = [list(row) for row in rows]  # Convert each row into a list

    # Insert the column headers as the first row in the sheet
    sheet.insert_row(df.columns, 1)

    # Append the rows from the DataFrame into the sheet only if there is data
    if data:
        sheet.append_rows(data)

    print(f"Data written to {sheet_name} successfully.")  # Print a success message

if __name__ == "__main__":
    # Google Sheets API authentication
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credenciales_json = './.ignore/google_sheets.json'  # Path to the credentials JSON file
    
    # Authenticate the client using the service account credentials
    creds = Credentials.from_service_account_file(credenciales_json, scopes=scope)
    client = gspread.authorize(creds)

    # Create a single PySpark session
    spark = SparkSession.builder.appName("CSV_to_GoogleSheets").getOrCreate()

    # Load data into Google Sheets by calling the function
    write_to_gsheets(client, "pyspark", "./data/cleaned_data_output/cleaned_data.csv", spark)
    write_to_gsheets(client, "analysis", "./data/cleaned_data_output/analysis/cleaned_data_2.csv", spark)