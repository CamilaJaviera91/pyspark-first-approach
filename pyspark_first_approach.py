from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import shutil
import os

spark = SparkSession.builder \
    .appName("Pyspark") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("./data/clean_data.csv", header=True, inferSchema=True)

df.printSchema()

count_files = df.count()

print(f"We are working with a file with '{count_files}' rows. \n")

df = df.withColumn("country_or_dependency", F.upper(F.col("country_or_dependency")))

total_population_value = df.agg(F.sum("population_2020").alias("total_population")).collect()[0]["total_population"]

df = df.withColumn("total_population", F.lit(total_population_value))

df = df.withColumn("percentage", F.round((F.col("population_2020") / F.col("total_population")) * 100, 2))

df = df.withColumn("percentage_formatted", F.concat(F.round(F.col("percentage"), 2), F.lit("%")))

output_folder = "./data/cleaned_data_output/"
output_file = "./data/cleaned_data_output/cleaned_data.csv"

df.coalesce(1)\
        .write\
        .mode("overwrite")\
        .option("header", "true")\
        .csv(output_folder)

for filename in os.listdir(output_folder):
    if filename.startswith("part-") and filename.endswith(".csv"):
        shutil.move(os.path.join(output_folder, filename), output_file)
        break

for filename in os.listdir(output_folder):
    if filename.startswith("_SUCCESS") or filename.endswith(".crc"):
        os.remove(os.path.join(output_folder, filename))

print(f"File saved as: {output_file}")