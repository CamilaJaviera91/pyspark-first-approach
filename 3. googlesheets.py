import gspread
from google.oauth2.service_account import Credentials
from pyspark.sql import SparkSession

def write_to_gsheets(client, sheet_name, csv_path, spark):
    """Reads a CSV using PySpark and writes it to Google Sheets."""
    
    # Open the Google Sheets document
    spreadsheet = client.open(sheet_name)
    sheet = spreadsheet.sheet1  

    # Clear the existing content
    sheet.clear()  

    # Read the CSV file into a PySpark DataFrame
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

    # Eliminar filas vacías
    df = df.na.drop(how="all")

    # Convertir el DataFrame en listas para Google Sheets
    rows = df.collect()
    data = [list(row) for row in rows]

    # Insertar encabezados en la primera fila
    sheet.insert_row(df.columns, 1)

    # Agregar filas solo si hay datos
    if data:
        sheet.append_rows(data)

    print(f"Data written to {sheet_name} successfully.")

if __name__ == "__main__":
    # Autenticación con Google Sheets API
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credenciales_json = './.ignore/google_sheets.json'
    
    creds = Credentials.from_service_account_file(credenciales_json, scopes=scope)
    client = gspread.authorize(creds)

    # Crear una sola sesión de PySpark
    spark = SparkSession.builder.appName("CSV_to_GoogleSheets").getOrCreate()

    # Cargar datos en hojas de cálculo
    write_to_gsheets(client, "pyspark", "./data/cleaned_data_output/cleaned_data.csv", spark)
    write_to_gsheets(client, "analysis", "./data/cleaned_data_output/analysis/cleaned_data_2.csv", spark)