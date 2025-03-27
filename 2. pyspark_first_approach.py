from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from fpdf import FPDF
import shutil
import os
import matplotlib.pyplot as plt
import hashlib
import logging

def get_file_hash(file_path):
    """Returns the SHA-256 hash of a file"""
    if not os.path.exists(file_path):
        return None
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        hasher.update(f.read())
    return hasher.hexdigest()

def new_col(spark):

    try:
        # Read CSV file into a DataFrame with header and schema inference
        df = spark.read.csv("./data/clean_data.csv", header=True, inferSchema=True)
        logger.info("CSV file successfully loaded.")

        # Count the number of rows in the DataFrame
        count_files = df.count()

        # Count the number of columns in the DataFrame
        count_col = len(df.columns)

        # Display the total number of rows and columns
        print(f"We are working with '{count_files}' rows and '{count_col}' columns.\n")

        # Convert 'country_or_dependency' column to uppercase
        df = df.withColumn("country_or_dependency", F.upper(F.col("country_or_dependency")))

        # Calculate total population for the year 2020
        total_population_value = df.agg(F.sum("population_2020").alias("total_population")).collect()[0]["total_population"]

        # Print the total population with formatted output
        print(f"Total Population: {"{:,}".format(total_population_value).replace(",", ".")} people in 2020\n")

        # Add a new column 'total_population' with the calculated value to each row
        df = df.withColumn("total_population", F.lit(total_population_value))

        # Calculate the percentage of the population for each row and round to 2 decimal places
        df = df.withColumn("percentage", F.round((F.col("population_2020") / F.col("total_population")) * 100, 2))

        # Format the percentage as a string with a '%' symbol
        df = df.withColumn("percentage_formatted", F.concat(F.round(F.col("percentage"), 0), F.lit("%")))

        df = df.withColumn("urban_pop", 
                            (F.col("population_2020") * 
                            (F.regexp_replace(F.col("urban_pop_%"), " %", "").cast("double") / 100)
                            ).cast("int"))
        
        df = df.withColumn("rural_pop_%",
                            F.when(
                                (F.col("urban_pop_%") == 0) | (F.col("urban_pop_%") == "0 %"), F.lit("0 %")
                            ).otherwise(
                                F.concat(F.lit(100 - F.regexp_replace(F.col("urban_pop_%"), " %", "").cast("double").cast("int")), F.lit(" %"))
                            )
                        )
        
        df = df.withColumn("rural_pop",
                            F.when(
                                (F.col("urban_pop_%") == 0) | (F.col("urban_pop_%") == "0 %"), 0
                            ).otherwise(
                                (F.col("population_2020") - 
                                (F.col("population_2020") * (F.regexp_replace(F.col("urban_pop_%"), " %", "").cast("double") / 100))
                                ).cast("int")
                            )
                        )
        
        # Print the schema of the DataFrame
        df.printSchema()

        # Define the output folder and file paths
        output_folder = "./data/cleaned_data_output/"
        output_file = "./data/cleaned_data_output/cleaned_data.csv"

        # Write the DataFrame to a CSV file, overwriting any existing files
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_folder)

        # Move the output file from the part-* file to the desired file name
        for filename in os.listdir(output_folder):
            if filename.startswith("part-") and filename.endswith(".csv"):
                shutil.move(os.path.join(output_folder, filename), output_file)
                break

        # Remove unnecessary files (_SUCCESS and .crc files) from the output folder
        for filename in os.listdir(output_folder):
            if filename.startswith("_SUCCESS") or filename.endswith(".crc"):
                os.remove(os.path.join(output_folder, filename))

        # Print confirmation message with the file path
        print(f"File saved as: {output_file}")
    
    except Exception as e:
        logger.error(f"Error processing the CSV file: {str(e)}")

def plot_data(spark):
    # Read the cleaned data CSV file using Spark
    df_data = spark.read.csv("./data/cleaned_data_output/cleaned_data.csv", header=True, inferSchema=True)

    # Convert the Spark DataFrame to a Pandas DataFrame to enable plotting with Matplotlib
    df_pandas = df_data.select("country_or_dependency", "percentage").toPandas()

    # Plot the top 10 countries by population percentage using a bar chart
    plt.figure(figsize=(10, 6))  # Set the figure size
    bars = plt.bar(df_pandas["country_or_dependency"][:10], df_pandas["percentage"][:10])
    plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
    plt.title("Top 10 Countries by Population Percentage")  # Set the title of the plot
    plt.xlabel("Country or Dependency")  # Label for the x-axis
    plt.ylabel("Percentage (%)")  # Label for the y-axis

     # Add values on top of the bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2.0, height, f'{height}%', ha='center', va='bottom', fontsize=10)

    plt.show()  # Display the plot

def plot_pie_chart(spark):
    # Read the cleaned data CSV file using Spark
    df_data = spark.read.csv("./data/cleaned_data_output/cleaned_data.csv", header=True, inferSchema=True)

    # Convert the Spark DataFrame to a Pandas DataFrame to enable plotting with Matplotlib
    df_pandas = df_data.select("country_or_dependency", "percentage").toPandas()

    # Get the top 10 countries by population percentage
    top_10 = df_pandas[:10]

    # Plot a pie chart
    plt.figure(figsize=(8, 8))
    plt.pie(top_10["percentage"], labels=top_10["country_or_dependency"], autopct='%1.1f%%', startangle=140, textprops={'fontsize': 10})
    plt.title("Population Distribution of Top 10 Countries (Percentage)", fontsize=14)
    plt.show()

def create_table(spark):
    # Read CSV with Spark
    df_data = spark.read.csv("./data/cleaned_data_output/cleaned_data.csv", header=True, inferSchema=True)
    df_data.createOrReplaceTempView("population_table")

    # Select top 10 countries by population
    result = spark.sql("""
        SELECT country_or_dependency AS country, 
               population_2020 AS population, 
               percentage_formatted AS percentage 
        FROM population_table
        ORDER BY population_2020 DESC
        LIMIT 10
    """)

    df_pandas = result.toPandas()

    # Create PDF
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 14)
    pdf.cell(0, 10, "Top 10 Countries by Population", ln=True, align='C')
    pdf.ln(10)

    pdf.set_font("Arial", "B", 12)
    pdf.cell(60, 10, "Country", 1, 0, 'C')
    pdf.cell(60, 10, "Population", 1, 0, 'C')
    pdf.cell(60, 10, "Percentage", 1, 1, 'C')

    pdf.set_font("Arial", size=12)
    for _, row in df_pandas.iterrows():
        pdf.cell(60, 10, row["country"], 1, 0, 'C')
        pdf.cell(60, 10, str(row["population"]), 1, 0, 'C')
        pdf.cell(60, 10, row["percentage"], 1, 1, 'C')

    pdf_file = "./data/cleaned_data_output/population_report.pdf"

    # Get old hash before saving
    old_hash = get_file_hash(pdf_file)

    # Save new PDF
    pdf.output(pdf_file)

    # Get new hash after saving
    new_hash = get_file_hash(pdf_file)

    if old_hash == new_hash:
        print("No changes detected. PDF not modified.")
    else:
        print(f"PDF saved as: {pdf_file}")

def create_report(spark):
    # Read the cleaned data CSV file using Spark and create a DataFrame
    df_data = spark.read.csv("./data/cleaned_data_output/cleaned_data.csv", header=True, inferSchema=True)

    # Register the DataFrame as a temporary SQL view to allow SQL queries
    df_data.createOrReplaceTempView("population_table")

    # Run a SQL query
    result = spark.sql("""
                        SELECT 
                            country_or_dependency AS `Country`, 
                            population_2020 AS `Population`, 
                            percentage_formatted AS `Percentage`,
                            `urban_pop_%` AS `% Urban Population`,
                            urban_pop AS `Urban Population`
                        FROM population_table
                        ORDER BY population_2020 DESC
                    """)

    # Show the result in the console for verification
    result.show()

    # Convert the Spark DataFrame to a Pandas DataFrame for easier manipulation
    df_pandas = result.toPandas()

    # Create a new PDF document
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=10)

    # Add a title to the PDF document
    pdf.set_font("Arial", "B", 10)
    pdf.cell(0, 10, "Countries", ln=True, align='C')
    pdf.ln(10)

    # Add table headers to the PDF
    pdf.set_font("Arial", "B", 10)
    pdf.cell(40, 10, "Country", 1, 0, 'C')
    pdf.cell(40, 10, "Population", 1, 0, 'C')
    pdf.cell(40, 10, "Percentage", 1, 0, 'C')
    pdf.cell(40, 10, "% Urban Population", 1, 0, 'C')
    pdf.cell(40, 10, "Urban Population", 1, 1, 'C')

    # Add the rows of data to the PDF
    pdf.set_font("Arial", size=10)
    for index, row in df_pandas.iterrows():
        pdf.cell(40, 10, row["Country"], 1, 0, 'C')
        pdf.cell(40, 10, str(row["Population"]), 1, 0, 'C')
        pdf.cell(40, 10, row["Percentage"], 1, 0, 'C')
        pdf.cell(40, 10, str(row["% Urban Population"]), 1, 0, 'C')
        pdf.cell(40, 10, str(row["Urban Population"]), 1, 1, 'C')


    # Define the file path to save the PDF
    pdf_file = "./data/cleaned_data_output/pdf_report.pdf"

     # Get old hash before saving
    old_hash = get_file_hash(pdf_file)

    # Save new PDF
    pdf.output(pdf_file)

    # Get new hash after saving
    new_hash = get_file_hash(pdf_file)

    if old_hash == new_hash:
        print("No changes detected. PDF not modified.")
    else:
        print(f"PDF saved as: {pdf_file}")

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("Pyspark").getOrCreate()

    # Set logging level to 'ERROR' to minimize logs
    spark.sparkContext.setLogLevel("ERROR")

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.StreamHandler()])
    
    logger = logging.getLogger(__name__)

    # Run the function if the script is executed directly
    new_col(spark)

    plot_data(spark)

    plot_pie_chart(spark)

    create_table(spark)

    create_report(spark)