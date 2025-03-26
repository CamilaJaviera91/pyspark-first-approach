from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import shutil
import os
import logging

def new_cols(spark):
    try:
        # Read CSV file into a DataFrame with header and schema inference
        df = spark.read.csv("./data/cleaned_data_output", header=True, inferSchema=True)
        logger.info("CSV file successfully loaded.")

        df = df.withColumn("population_2024", (F.col("population_2020") * 1.08).cast("int"))
        
        df = df.withColumn("population_2023", (F.col("population_2020") * 1.06).cast("int"))

        df = df.withColumn("population_2022", (F.col("population_2020") * 1.04).cast("int"))

        df = df.withColumn("population_2021", (F.col("population_2020") * 1.02).cast("int"))

        df = df.withColumn("population_2019", (F.col("population_2020") * 0.98).cast("int"))

        df = df.withColumn("population_2018", (F.col("population_2020") * 0.96).cast("int"))

        df = df.withColumn("population_2017", (F.col("population_2020") * 0.94).cast("int"))

        df = df.withColumn("population_2016", (F.col("population_2020") * 0.92).cast("int"))

        df = df.withColumn("population_2015", (F.col("population_2020") * 0.90).cast("int"))

        df = df.withColumn("population_density", F.col("population_2020") / F.col("land_area_km²"))
        
        df = df.withColumn("population_density", F.round(F.col("population_density"), 2))

        df = df.withColumn("growth_rate", (F.col("population_2020") - F.col("population_2015")) / (F.col("population_2015") * 100))
        
        df = df.withColumn("growth_rate", F.round(F.col("growth_rate"), 2))

        # Define the output folder and file paths
        output_folder = "./data/cleaned_data_output/analysis/"
        output_file = "./data/cleaned_data_output/analysis/cleaned_data_2.csv"

        # Write the DataFrame to a CSV file, overwriting any existing files
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_folder)
        
        # Print the schema of the DataFrame
        df.printSchema()

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

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("Pyspark2").getOrCreate()

    # Set logging level to 'ERROR' to minimize logs
    spark.sparkContext.setLogLevel("ERROR")

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.StreamHandler()])
    
    logger = logging.getLogger(__name__)

    # Run the function if the script is executed directly

    previous_years(spark)