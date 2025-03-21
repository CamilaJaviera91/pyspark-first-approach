from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import shutil
import os
import matplotlib.pyplot as plt

def new_col(spark):

    # Read CSV file into a DataFrame with header and schema inference
    df = spark.read.csv("./data/clean_data.csv", header=True, inferSchema=True)

    print("\n")

    # Print the schema of the DataFrame
    df.printSchema()

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
    df = df.withColumn("percentage_formatted", F.concat(F.round(F.col("percentage"), 2), F.lit("%")))

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

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("Pyspark").getOrCreate()

    # Set logging level to 'ERROR' to minimize logs
    spark.sparkContext.setLogLevel("ERROR")

    # Run the function if the script is executed directly
    new_col(spark)

    plot_data(spark)