from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
import pandas as pd
import matplotlib.pyplot as plt

def correlation_analysis(df):

    # Show max and min of the two variables that we are using 
    df.select(["yearly_change", "migrants_net"]).summary("count", "min", "max").show()

    # Convert 'yearly_change' column to numeric (removing "%")
    df = df.withColumn("yearly_change",  
                       F.regexp_replace(F.col("yearly_change"), " %", "").cast("double").cast("int"))

    # Compute correlation between annual change and net migration
    correlation = df.stat.corr("yearly_change", "migrants_net")
    print(f"Correlation between yearly change and net migration: {round(correlation, 4)}")

def growth_country(df):

    # Convert PySpark DataFrame to Pandas
    df_pandas = df.select("country_or_dependency", "population_2015", "population_2016", "population_2017", 
                          "population_2018", "population_2019", "population_2020", "population_2021", 
                          "population_2022", "population_2023").toPandas()
    
    # Select a country (example: India)
    df_country = df_pandas[df_pandas["country_or_dependency"] == "CHINA"]

    # Plot the data
    years = ["population_2015", "population_2016", "population_2017", "population_2018", "population_2019", 
             "population_2020", "population_2021", "population_2022", "population_2023"]
    values = df_country[years].values[0]

    plt.plot(years, values, marker="o", linestyle="-")
    plt.xlabel("Year")
    plt.ylabel("Population")
    plt.title("Population Growth in China")
    plt.show()

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("Pyspark3").getOrCreate()

    # Set logging level to 'ERROR' to minimize logs
    spark.sparkContext.setLogLevel("ERROR")

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.StreamHandler()])
    
    logger = logging.getLogger(__name__)

    # Read CSV file into a DataFrame with header and schema inference
    df = spark.read.csv("./data/cleaned_data_output/analysis/cleaned_data_2.csv", 
                        header=True, inferSchema=True)
    logger.info("CSV file successfully loaded.")

    # Run the function if the script is executed directly

    correlation_analysis(df)
    
    growth_country(df)