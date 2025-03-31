from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import functions as F
import logging
from pyspark.sql.window import Window

def train_population_model(df, model_type="linear"):
    
    # Convert necessary columns to numeric (remove "%" where needed)
    df = df.withColumn("Country", 
                       F.upper(F.col("country_or_dependency")))
    
    df = df.withColumn("yearly_change",  
                       F.regexp_replace(F.col("yearly_change"), " %", "").cast("double"))
    
    df = df.withColumn("fertility_rate", 
                       F.col("fert_rate").cast("double"))
    
    df = df.withColumn("population_2020", 
                       F.col("population_2020").cast("int"))
    
    df = df.withColumn("migrants_net_%", 
                       F.round((F.col("migrants_net") / F.col("net_change")), 2))

    # Drop rows with missing values
    df = df.na.drop()

    # Assemble feature vector
    feature_cols = ["yearly_change", "fertility_rate", "migrants_net_%"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df = assembler.transform(df)

    window_spec = Window.orderBy(F.monotonically_increasing_id())
    df = df.withColumn("index", F.row_number().over(window_spec))

    # Split data into train/test sets
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

    # Train the model
    if model_type == "linear":
        model = LinearRegression(featuresCol="features", labelCol="population_2020")
    else:
        raise ValueError("Only 'linear' model is supported for now")

    trained_model = model.fit(train_data)

    # Make predictions
    predictions = trained_model.transform(test_data)
    predictions = predictions.withColumn("prediction", F.round("prediction", 0).cast("long"))

    # Show ordered results
    # Mostrar resultados sin ordenar por población
    predictions.orderBy("index").select("Country", "features", "population_2020", "prediction").show(10)

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("Pyspark5").getOrCreate()

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
    train_population_model(df, model_type="linear")