from pyspark.sql import SparkSession
from delta import *
from csv_pipeline import CsvPipeline  # Make sure this matches the name of your Python file containing the class

if __name__ == "__main__":
    # Initialize a Spark session
    

    spark = SparkSession.builder.appName("CSVPIpeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").getOrCreate()
    #.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    #builder = SparkSession.builder.appName("MyApp") \
    #.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    #.config("spark.jars.package", "io.delta:delta-core_2.12:1.2.0") 
    #.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    #spark = configure_spark_with_delta_pip(builder).getOrCreate()

    csv_file_path = "/Users/augustobarbosa/Py_Projects/CSV-Pipeline/csv-pipeline/docs"  # Change this to the path of your actual CSV file
    options = {'header': 'true', 'inferSchema': 'true'}  # Adjust these options as needed

    # Create an instance of the CsvPipeline class
    csv_pipeline = CsvPipeline(spark, csv_file_path, options)
    csv_pipeline.read_csv_pipeline() 

    
    # Stop the Spark session
    spark.stop()

    # scala Scala code runner version 3.3.1 -- Copyright 2002-2023, LAMP/EPFL
    # spark scala> spark.version
    # res0: String = 3.5.1
    ## Using Scala version 2.12.18 (Java HotSpot(TM) 64-Bit Server VM, Java 21.0.2)